<?php
// This is a PHP client for the GTSDB server
class GTSDBClient {
    private $socket;
    private $host;
    private $port;

    private $connected = false;

    public function __construct($host, $port) {
        $this->host = $host;
        $this->port = $port;
    }

    public function connect() {
        $this->socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($this->socket === false) {
            throw new Exception("socket_create() failed: reason: " . socket_strerror(socket_last_error()));
        }

        $result = socket_connect($this->socket, $this->host, $this->port);
        if ($result === false) {
            throw new Exception("socket_connect() failed.\nReason: ($result) " . socket_strerror(socket_last_error($this->socket)));
        }

        $this->connected = true;
    }

    private function disconnect() {
        socket_close($this->socket);
    }

    private function send($data) {
        socket_write($this->socket, $data, strlen($data));
    }

    private function receive() {
        $response = "";
        while ($out = socket_read($this->socket, 2048)) {
            // check if output is complete by checking if it ends with a newline
            if (substr($out, -1) == "\n") {
                $response .= $out;
                break;
            }
            $response .= $out;
        }
        return $response;
    }

    public function query($key, $tags, $start, $end) {
        if (!$this->connected) {
            $this->connect();
        }

        $this->send("");
    }
}
