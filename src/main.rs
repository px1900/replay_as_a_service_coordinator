use core::time;
use std::sync::{Arc};
use std::{env, string};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex, RwLock};


type SharedReplayServers = Arc<RwLock<Vec<Arc< (Mutex<TcpStream>,String) >>>>;

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
            replay_workers_locked.push(Arc::new((tokio::sync::Mutex::new(new_replay_worker), socket.peer_addr().unwrap().ip().to_string()+socket.peer_addr().unwrap().port().to_string().as_str())));
            println!("{} {}, timeline_listener: replay_workers.len() = {}", file!(), line!(), replay_workers_locked.len());
        }

        let replay_workers_for_task = Arc::clone(&replay_workers);

        // Spawn a new task to handle the connection
        let replay_workers_for_task = Arc::clone(&replay_workers_for_task);
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

    // let (tx_request, mut rx_request) = mpsc::channel::<Vec<u8>>(8192*20);
    // let (tx_response, mut rx_response) = mpsc::channel::<Vec<u8>>(8192*20);

    // Get the peer address
    let peer_addr = socket.peer_addr()?.ip().to_string();
    println!("New connection from {}", peer_addr);
    // Spawn a new task to start a socket client with the peer address and port 64000
    // tokio::spawn( async {
    //     match distribute_task_to_workers(replay_workers).await {
    //         Ok(_) => {},
    //         Err(e) => {
    //             println!("Error when distributing task to workers: {:?}", e);
    //         }
    //     }
    // });

    return handle_incoming_data(socket, replay_workers).await;

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
    let mut response = vec![0u8; 8192];
    stream.read(&mut response).await?;
    println!("Got response from peer server");

    Ok(stream)
}

async fn distribute_task_to_workers(replay_workers: SharedReplayServers, sender_addr: String, data: Vec<u8>) -> io::Result<Vec<u8>> {

    let mut iter_num = 0;
    loop {
        let replay_workers_locked = replay_workers.read().await;
        // Won't send the task back to the sender
        if replay_workers_locked[iter_num].1 == sender_addr {
            println!("distribute_task_to_workers: iter_num = {}, sender_ip = {}, replay_worker_ip = {}, skip this worker", iter_num, sender_addr, replay_workers_locked[iter_num].1);
            iter_num = (iter_num + 1) % replay_workers_locked.len();
            // sleep 10ms
            tokio::time::sleep(time::Duration::from_millis(10)).await;
            continue;
        }

        if let Ok(replay_worker_stream) = replay_workers_locked[iter_num].0.try_lock() {
            println!("worker_listener: try to distribute this task to {} worker", iter_num);
            match distribute_task_to_one_server(data.clone(), replay_worker_stream).await {
                Ok(response) => {
                    return Ok(response);
                },
                Err(e) => {
                    println!("worker_listener: Error when distributing task to one worker: {:?}", e);
                }
            }
        } else {
            iter_num = (iter_num + 1) % replay_workers_locked.len();
            // sleep 10ms
            tokio::time::sleep(time::Duration::from_millis(10)).await;
        }
        
        ; // Don't know why we need this semicolon
    } 
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

async fn handle_incoming_data(mut socket: TcpStream, replay_workers: SharedReplayServers) -> io::Result<()> {
    loop {
        let replay_workers = Arc::clone(&replay_workers);

        println!("{} {}, timeline_listener: waiting for data from the socket", file!(), line!());

        // Read the incoming data from the socket, then write it to Sender
        let mut buffer = [0; 8192*10];
        let mut total_read_size: usize = socket.read(&mut buffer).await?;
        if total_read_size == 0 {
            return Ok(());
        }
        
        // Convert the buffer[1..5] to u32
        let target_buffer_size: u32 = u32::from_be_bytes(buffer[1..5].try_into().unwrap());
        println!("{} {}, timeline_listener: target_buffer_size = {}, received {} bytes", file!(), line!(), target_buffer_size, total_read_size);

        let mut to_sent_msg = Vec::<u8>::new();
        to_sent_msg.extend_from_slice(&buffer[..total_read_size]);
        while total_read_size < (target_buffer_size+1) as usize  {
            let n = socket.read(&mut buffer).await?;
            to_sent_msg.extend_from_slice(&buffer[..n]);
            total_read_size += n;
            println!("{} {}, timeline_listener: received {} bytes from the socket", file!(), line!(), n);
        }

        let response = distribute_task_to_workers(replay_workers, socket.peer_addr().unwrap().ip().to_string()+socket.peer_addr().unwrap().to_string().as_str(), to_sent_msg).await?;

        socket.write_all(&response).await?;
        socket.flush().await?;
    }
}

