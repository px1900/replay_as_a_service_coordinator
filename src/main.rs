use core::time;
use std::sync::{Arc};
use std::env;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex, RwLock};


type SharedReplayServers = Arc<RwLock<Vec<Arc<Mutex<TcpStream>>>>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Bind the TCP listener to the address
    let listener: TcpListener = TcpListener::bind("10.145.21.36:18080").await?;

    let replay_workers: SharedReplayServers = Arc::new(RwLock::new(Vec::new()));

    println!("Server listening on 10.145.21.36:18080");

    loop {
        // Accept an incoming connection
        let (socket, _) = listener.accept().await?;

        // Get the peer address
        if let Ok(peer_addr) = socket.peer_addr() {
            println!("New connection from {}", peer_addr);
        }

        // One timeline try to establish a NEW connection with the coordinator
        // One timeline may establish multiple connections with the coordinator, for each time, 
        // the coordinator will register a new replay worker in its server
        let new_replay_worker = match register_worker_in_peer_server(socket.peer_addr().unwrap().ip().to_string()).await {
            Ok(stream) => {
                println!("Connected to replay_worker, peer_addr = {}", socket.peer_addr().unwrap().ip().to_string());
                stream
            },
            Err(e) => {
                println!("Error when connecting to replay worker: {:?}", e);
                return Ok(());
            }
        };

        // We remember the timeline's connection in the replay_workers list
        {
            // Add the new replay server to the replay_workers
            let mut replay_workers_locked = replay_workers.write().await;
            replay_workers_locked.push(Arc::new(tokio::sync::Mutex::new(new_replay_worker)));
        }

        let replay_workers_for_task = Arc::clone(&replay_workers);

        // Spawn a new task to handle the connection
        tokio::spawn(async {
            if let Err(e) = handle_connection(socket, replay_workers_for_task).await {
                eprintln!("Failed to handle connection; err = {:?}", e);
            }
        });
    }
}

// Function to handle the connection
async fn handle_connection(mut socket: TcpStream, replay_workers: SharedReplayServers) -> io::Result<()> {


    // Define a mscp::channel
    // Timeline --timeline_socket--> timeline_listening_thread --tx_request--> * --rx_request--> server_listening_thread --> woker_socket --> Worker
    // Timeline <--timeline_socket-- timeline_listening_thread <--rx_response- * <-tx_response-- server_listening_thread <-- worker_socket -- Worker

    let (tx_request, mut rx_request) = mpsc::channel::<Vec<u8>>(8192*20);
    let (tx_response, mut rx_response) = mpsc::channel::<Vec<u8>>(8192*20);

    // Get the peer address
    let peer_addr = socket.peer_addr()?.ip().to_string();
    println!("New connection from {}", peer_addr);
    // Spawn a new task to start a socket client with the peer address and port 64000
    tokio::spawn( async {
        match distribute_task_to_workers(rx_request, tx_response, replay_workers).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error when distributing task to workers: {:?}", e);
            }
        }
    });

    return handle_incoming_data(socket, tx_request, rx_response).await;

}

async fn register_worker_in_peer_server(peer_addr: String) -> io::Result<TcpStream> {
    println!("Connecting to replay server: {}", format!("{}:64000", peer_addr));
    let mut stream = TcpStream::connect(format!("{}:64000", peer_addr)).await?;

    let tenant_id_key = "tenant_id";
    let tenant_id_env = env::var(tenant_id_key).unwrap();
    let timeline_id_key = "timeline_id";
    let timeline_id_env = env::var(timeline_id_key).unwrap();

    // Before forwarding the data, we need to initialize the connection with the peer server
    let init_string = format!("pagestream_v2 {} {}", tenant_id_env, timeline_id_env);
    let init_string_len = init_string.len() as u32 + 4;
    let init_string_len_buf = init_string_len.to_be_bytes();
    // The sent msg should be Q + init_string_len_buf + init_string
    let mut init_string_buf = vec![0u8; 5];
    init_string_buf[0] = b'Q';
    init_string_buf[1..5].copy_from_slice(&init_string_len_buf);
    init_string_buf.extend(init_string.as_bytes());
    // Send this init_string to the peer server
    stream.write_all(&init_string_buf).await?;
    stream.flush().await?;
    println!("Sent init_string to peer server: {}", init_string);

    // Read response from the peer server
    let mut response = vec![0u8; 1024];
    stream.read(&mut response).await?;
    println!("Got response from peer server");

    Ok(stream)
}

async fn distribute_task_to_workers(mut rx_request: mpsc::Receiver<Vec<u8>>, tx_response: mpsc::Sender<Vec<u8>>, replay_workers: SharedReplayServers) -> io::Result<()> {

    while let Some(data) = rx_request.recv().await {
        println!("{} {}, worker_listener: received {} bytes from the channel", file!(), line!(), data.len());
        // Let's acquire a replay server to do the task
        let replay_workers_locked = replay_workers.read().await;
        let mut iter_num = 0;
        loop {
            if let Ok(replay_worker_stream) = replay_workers_locked[iter_num].try_lock() {
                println!("worker_listener: try to distribute this task to {} worker", iter_num);
                match distribute_task_to_one_server(data.clone(), replay_worker_stream).await {
                    Ok(response) => {
                        println!("{} {}, worker_listener: sent {} bytes to the channel", file!(), line!(), response.len());
                        if let Err(e) = tx_response.send(response).await {
                            println!("worker_listener: Error when sending response to channel: {:?}", e);
                        }
                        break;
                    },
                    Err(e) => {
                        println!("worker_listener: Error when distributing task to one worker: {:?}", e);
                    }
                }
            } else {
                iter_num = (iter_num + 1) % replay_workers_locked.len();
            }
        }
        
        // println!("{} {}, client: got response from peer server: {:?}", file!(), line!(), response);
    }
    Ok(())
}

async fn distribute_task_to_one_server(data: Vec<u8>, mut stream: tokio::sync::MutexGuard<'_, TcpStream>) -> io::Result<Vec<u8>> {

    match stream.write_all(&data).await {
        Ok(_) => {
            println!("{} {}, worker_listener: sent {} bytes to the replay_worker", file!(), line!(), data.len());
            stream.flush().await?;
        },
        Err(e) => {
            println!("Error when sending data to peer server: {:?}", e);
            return Err(e);
        }
    }
    // Read response from the peer server
    let mut response = vec![0u8; 8192*20];
    match stream.read(&mut response).await {
        Ok(byte_num) => {
            println!("{} {}, worker_listener: receive {} bytes from the replay_worker", file!(), line!(), byte_num);
            return Ok(response[..byte_num].to_vec());
        },
        Err(e) => {
            println!("Error when reading response from peer server: {:?}", e);
            return Err(e);
        }
    }
}

async fn handle_incoming_data(mut socket: TcpStream, tx_request: mpsc::Sender<Vec<u8>>, mut rx_response: mpsc::Receiver<Vec<u8>>) -> io::Result<()> {
    loop {

        // Read the incoming data from the socket, then write it to Sender
        let mut buffer = [0; 8192*20];
        let mut n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        
        // Convert the buffer[1..5] to u32
        let target_buffer_size: u32 = u32::from_be_bytes(buffer[1..5].try_into().unwrap());
        while n < (target_buffer_size+1) as usize  {
            let n2 = socket.read(&mut buffer[n..]).await?;
            n += n2;
        }

        println!("{} {}, timeline_listener: received {} bytes from the socket", file!(), line!(), n);
        match tx_request.send(buffer[..n].to_vec()).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error when sending data to channel: {:?}", e);
                return Ok(());
            }
        }
        println!("{} {}, timeline_listener: sent {} bytes to the channel", file!(), line!(), n);

        match rx_response.recv().await {
            Some(response) => {
                println!("{} {}, timeline_listener: received {} bytes from the channel", file!(), line!(), response.len());
                socket.write_all(&response).await?;
                socket.flush().await?;
                println!("{} {}, timeline_listener: sent {} bytes to the socket", file!(), line!(), response.len());
            },
            None => {
                println!("{} {}, timeline_listener: received None from the channel", file!(), line!());
            }

        }

    }
}

