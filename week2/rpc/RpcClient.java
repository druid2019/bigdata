package com.geek;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

public class RpcClient {
    public static void main(String[] args) {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(RpcServer.IP, RpcServer.PORT);

        try {
            IProxyProtocol proxy = RPC.waitForProxy(IProxyProtocol.class, IProxyProtocol.versionID, inetSocketAddress, new Configuration());
            Scanner sc = new Scanner(System.in);
            System.out.println("请输入学号：");
            String name = proxy.findName(sc.next());
            System.out.println("学生的名字是：" + name);
            RPC.stopProxy(proxy);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
