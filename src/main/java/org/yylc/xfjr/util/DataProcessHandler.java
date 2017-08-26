package org.yylc.xfjr.util;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class DataProcessHandler {
    private static final int MULIPLE_CONT = 10;    // 10个多粮仓
    private Random random = new Random();
    private MultipleImportDatas[] multipleImportDatas = new MultipleImportDatas[MULIPLE_CONT];    // 10个多粮仓入库

    public DataProcessHandler() {
        for (int i = 0; i < MULIPLE_CONT; i++) {
            multipleImportDatas[i] = new MultipleImportDatas();
        }
    }

    private void 处理来的每一条数据(String dataTypeCode, List<Map<String, String>> datas) {
        int index = random.nextInt(MULIPLE_CONT);    // 随机选择1个多粮仓入库
        multipleImportDatas[index].concurrentImportDatas(dataTypeCode, datas, false);    // 线程安全入数据。入库需满仓
    }

}