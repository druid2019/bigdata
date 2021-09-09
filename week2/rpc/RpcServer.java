package com.geek;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

public class RpcServer {
    public static int PORT = 5096;
    public static String IP = "127.0.0.1";

    public static void main(String[] args) throws IOException {
        RpcProxy proxy = new RpcProxy();
        Configuration conf = new Configuration();
        Server server = new RPC.Builder(conf).setProtocol(IProxyProtocol.class).setInstance(new RpcProxy())
                .setBindAddress(IP).setPort(PORT).build();
        server.start();
    }
}
