package com.geek;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class RpcProxy implements IProxyProtocol{
    @Override
    public int add(int number1, int number2) {
        System.out.println("number1 = " + number1 + " number2 = " + number2);
        int result = number1 + number2;
        return result;
    }

    @Override
    public String findName(String studentId) {
        if ("20210123456789".equals(studentId)) return "心心";
        if ("20210000000000".equals(studentId)) return "null";
        if ("G20200343150041".equals(studentId)) return "cal";
        return "其他";
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        System.out.println("Proxy.protocolVersion = " + IProxyProtocol.versionID);
        return IProxyProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
