use std::cmp::max;
use std::net::IpAddr;
use std::sync::mpsc;

pub fn get_local_ip() -> Result<IpAddr, std::io::Error> {
    let ip = std::env::var("DATANODE_IP");
    if ip.is_ok() {
        Ok(ip.unwrap().parse().unwrap())
    } else {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        let local_addr = socket.local_addr()?;
        Ok(local_addr.ip())
    }
}

pub struct ConcurrencyLimit {
    recv: async_channel::Receiver<()>,
    send: async_channel::Sender<()>
}

// todo: implement drop
impl ConcurrencyLimit {
    pub fn new(max_concurrency: i32) -> Self {
        let (send, recv) = async_channel::bounded(max_concurrency as usize);
        ConcurrencyLimit {
            recv,
            send
        }
    }

    pub async fn get_token(&self) {
        self.send.send(()).await;
    }

    pub async fn return_token(&self) {
        self.recv.recv().await;
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn drop_test() {

    }
}