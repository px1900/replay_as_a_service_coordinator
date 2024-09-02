use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> io::Result<()> {
    // Bind the TCP listener to the address
    let listener = TcpListener::bind("127.0.0.1:18080").await?;

    println!("Server listening on 127.0.0.1:18080");

    loop {
        // Accept an incoming connection
        let (socket, _) = listener.accept().await?;

        // Get the peer address
        if let Ok(peer_addr) = socket.peer_addr() {
            println!("New connection from {}", peer_addr);
        }

        // Spawn a new task to handle the connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Failed to handle connection; err = {:?}", e);
            }
        });
    }
}

// Function to handle the connection
async fn handle_connection(mut socket: TcpStream) -> io::Result<()> {


    // Define a mscp::channel
    let (tx_request, mut rx_request) = mpsc::channel::<Vec<u8>>(8192);
    let (tx_response, mut rx_response) = mpsc::channel::<Vec<u8>>(16384);

    // Get the peer address
    let peer_addr = socket.peer_addr()?.ip().to_string();
    println!("New connection from {}", peer_addr);
    // Spawn a new task to start a socket client with the peer address and port 64000
    tokio::spawn( async {
        match distribute_data_back_to_servers(rx_request, tx_response, peer_addr).await {
            Ok(_) => {},
            Err(e) => {
                println!("Error when distributing data back to servers: {:?}", e);
            }
        }
    });

    return handle_incoming_data(socket, tx_request, rx_response).await;

}

async fn distribute_data_back_to_servers(mut rx_request: mpsc::Receiver<Vec<u8>>, tx_response: mpsc::Sender<Vec<u8>>, peer_addr: String) -> io::Result<()> {
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
    println!("Got response from peer server: {:?}", String::from_utf8_lossy(&response));


    while let Some(data) = rx_request.recv().await {
        println!("{} {}, client: received {} bytes from the channel", file!(), line!(), data.len());
        match stream.write_all(&data).await {
            Ok(_) => {
                println!("{} {}, client: sent {} bytes to the peer server, data = {:?}", file!(), line!(), data.len(), data);
                stream.flush().await?;
                println!("{} {}, client: flushed {} bytes to the peer server", file!(), line!(), data.len());
            },
            Err(e) => {
                println!("Error when sending data to peer server: {:?}", e);
                return Ok(());
            }
        }
        // Read response from the peer server
        let mut response = vec![0u8; 8192*2];
        match stream.read(&mut response).await {
            Ok(byte_num) => {
                println!("{} {}, client: received {} bytes from the peer server", file!(), line!(), byte_num);
                match tx_response.send(response[..byte_num].to_vec()).await {
                    Ok(_) => {},
                    Err(e) => {
                        println!("Error when sending response to channel: {:?}", e);
                        return Ok(());
                    }
                }
            },
            Err(e) => {
                println!("Error when reading response from peer server: {:?}", e);
                return Ok(());
            }
        }
        // println!("{} {}, client: got response from peer server: {:?}", file!(), line!(), response);
    }
    Ok(())
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

        // let mut record_num_buf = [0u8; 4];
        // let stream_read_len = socket.read_exact(&mut record_num_buf).await?;
        // if stream_read_len == 0 {
        //     return Ok(());
        // }

        // let record_num = u32::from_be_bytes(record_num_buf) as usize;
        // println!("record_num: {}", record_num);
    
        // for i in 0..record_num {
        //     let mut record_len_buf = [0u8; 4];
        //     socket.read_exact(&mut record_len_buf).await?;
        //     let record_len = u32::from_be_bytes(record_len_buf) as usize;
        //     let mut record_buf = vec![0u8; record_len];
        //     socket.read_exact(&mut record_buf).await?;
        //     println!("got one record with len = {}", record_len);
        // }

        // let mut image_len_buf = [0u8; 4];
        // let stream_read_len = socket.read_exact(&mut image_len_buf).await?;
        // if stream_read_len == 0 {
        //     return Ok(());
        // }

        // let image_len = u32::from_be_bytes(image_len_buf) as usize;

        // if image_len != 0 {
        //     println!("will an image with length {}", image_len);
        //     let mut image_buf = vec![0u8; image_len];
        //     socket.read_exact(&mut image_buf).await?;
        //     println!("read an image with length {}", image_len);
        // }
    }
}

