use std::sync::{Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex};


type SharedReplayServers = Arc<Mutex<Vec<Arc<Mutex<TcpStream>>>>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Bind the TCP listener to the address
    let listener: TcpListener = TcpListener::bind("10.145.21.43:18080").await?;

    let replay_servers: SharedReplayServers = Arc::new(Mutex::new(Vec::new()));

    println!("Server listening on 10.145.21.43:18080");

    loop {
        // Accept an incoming connection
        let (socket, _) = listener.accept().await?;

        // Get the peer address
        if let Ok(peer_addr) = socket.peer_addr() {
            println!("New connection from {}", peer_addr);
        }

        let new_replay_server = match connect_to_replay_server(socket.peer_addr().unwrap().ip().to_string()).await {
            Ok(stream) => {
                println!("Connected to replay_server, peer_addr = {}", socket.peer_addr().unwrap().ip().to_string());
                stream
            },
            Err(e) => {
                println!("Error when connecting to replay server: {:?}", e);
                return Ok(());
            }
        };

        {
            // Add the new replay server to the replay_servers
            let mut replay_servers_locked = replay_servers.lock().await;
            replay_servers_locked.push(Arc::new(Mutex::new(new_replay_server)));
        }

        let replay_servers_for_task = Arc::clone(&replay_servers);

        // Spawn a new task to handle the connection
        tokio::spawn(async {
            if let Err(e) = handle_connection(socket, replay_servers_for_task).await {
                eprintln!("Failed to handle connection; err = {:?}", e);
            }
        });
    }
}

// Function to handle the connection
async fn handle_connection(mut socket: TcpStream, replay_servers: SharedReplayServers) -> io::Result<()> {


    // Define a mscp::channel
    let (tx_request, mut rx_request) = mpsc::channel::<Vec<u8>>(8192);
    let (tx_response, mut rx_response) = mpsc::channel::<Vec<u8>>(16384);

    // Get the peer address
    let peer_addr = socket.peer_addr()?.ip().to_string();
    println!("New connection from {}", peer_addr);
    // Spawn a new task to start a socket client with the peer address and port 64000
    tokio::spawn( async {
        match distribute_data_back_to_servers(rx_request, tx_response, replay_servers).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error when distributing data back to servers: {:?}", e);
            }
        }
    });

    return handle_incoming_data(socket, tx_request, rx_response).await;

}

async fn connect_to_replay_server(peer_addr: String) -> io::Result<TcpStream> {
    println!("Connecting to replay server: {}", format!("{}:64000", peer_addr));
    let mut stream = TcpStream::connect(format!("{}:64000", peer_addr)).await?;

    // Before forwarding the data, we need to initialize the connection with the peer server
    let tenant_id = "fe705bb4743223a63ec8690f20b2e518";
    let timeline_id = "4d285e6c0aea3f18c41e219cf2bab4e7";
    let init_string = format!("pagestream_v2 {} {}", tenant_id, timeline_id);
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

async fn distribute_data_back_to_servers(mut rx_request: mpsc::Receiver<Vec<u8>>, tx_response: mpsc::Sender<Vec<u8>>, replay_servers: SharedReplayServers) -> io::Result<()> {

    while let Some(data) = rx_request.recv().await {
        // Let's acquire a replay server to do the task
        let replay_servers_locked = replay_servers.lock().await;
        let mut iter_num = 0;
        loop {
            if let Ok(replay_server_stream) = replay_servers_locked[iter_num].try_lock() {
                match distribute_task_to_one_server(data.clone(), replay_server_stream).await {
                    Ok(response) => {
                        tx_response.send(response).await;
                        break;
                    },
                    Err(e) => {
                        println!("Error when distributing task to one server: {:?}", e);
                    }
                }
            } else {
                iter_num = (iter_num + 1) % replay_servers_locked.len();
            }
        }

        println!("{} {}, client: received {} bytes from the channel", file!(), line!(), data.len());
        
        // println!("{} {}, client: got response from peer server: {:?}", file!(), line!(), response);
    }
    Ok(())
}

async fn distribute_task_to_one_server(data: Vec<u8>, mut stream: tokio::sync::MutexGuard<'_, TcpStream>) -> io::Result<Vec<u8>> {

    match stream.write_all(&data).await {
            Ok(_) => {
                println!("{} {}, client: sent {} bytes to the peer server, data = {:?}", file!(), line!(), data.len(), data);
                stream.flush().await?;
                println!("{} {}, client: flushed {} bytes to the peer server", file!(), line!(), data.len());
            },
            Err(e) => {
                println!("Error when sending data to peer server: {:?}", e);
                return Err(e);
            }
        }
        // Read response from the peer server
        let mut response = vec![0u8; 8192*2];
        match stream.read(&mut response).await {
            Ok(byte_num) => {
                println!("{} {}, client: received {} bytes from the peer server", file!(), line!(), byte_num);
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
        let mut buffer = [0; 8192*16];
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        println!("{} {}, server: received {} bytes from the socket", file!(), line!(), n);
        match tx_request.send(buffer[..n].to_vec()).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error when sending data to channel: {:?}", e);
                return Ok(());
            }
        }
        println!("{} {}, server: sent {} bytes to the channel", file!(), line!(), n);

        match rx_response.recv().await {
            Some(response) => {
                println!("{} {}, server: received {} bytes from the channel", file!(), line!(), response.len());
                socket.write_all(&response).await?;
                socket.flush().await?;
                println!("{} {}, server: sent {} bytes to the socket", file!(), line!(), response.len());
            },
            None => {
                println!("{} {}, server: received None from the channel", file!(), line!());
            }

        }

    }
}

