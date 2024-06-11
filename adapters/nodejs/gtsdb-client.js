// this is the node.js client that will be used to interact with the GTSDB server
// it creates a tcp connection to the server and sends the data to the server

const net = require('net');

class GTSDBClient {
    constructor(host, port, username, password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }
    
    connect() {
        this.client = new net.Socket();
        this.client.connect(this.port, this.host, () => {
           console.log('Connected to GTSDB server');

        });
    }
    
    send(data) {
        this.client.write(data);
    }
    
    close() {
        this.client.destroy();
    }

    addData(sensorId,timestamp,value) {
        let data = `${sensorId},${timestamp},${value}\n`;
        this.send(data);
    }
    
    queryData(sensorId,from,to,downsampling) {
        let data = `${sensorId},${from},${to},${downsampling}`;
        this.send(data);
    }
    
    subscribe(sensorId) {
        let data = `subscribe,${sensorId}`;
        this.send(data);
    }
}

module.exports = GTSDBClient;
