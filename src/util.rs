use std::net::IpAddr;

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

#[cfg(test)]
mod test {

    #[test]
    fn pipe_test() {

    }
}