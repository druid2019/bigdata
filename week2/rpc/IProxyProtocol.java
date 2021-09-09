package com.geek;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IProxyProtocol extends VersionedProtocol {
    static final long versionID = 7L;

    int add(int number1, int number2);

    String findName(String studentId);
}
