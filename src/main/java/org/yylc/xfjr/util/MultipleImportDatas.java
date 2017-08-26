package org.yylc.xfjr.util;

import org.eclipse.persistence.exceptions.DatabaseException;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MultipleImportDatas    // 多粮仓线程安全入库
{
    private ConcurrentHashMap<String, List<Map<String, String>>> inputBuffer = new ConcurrentHashMap<String, List<Map<String, String>>>();    // 各粮仓，按数据类型划分
    private ConcurrentHashMap<String, Date> inputBufferDate = new ConcurrentHashMap<String, Date>();    // 各粮仓，最近清仓时间
    private final int BUFFER_SIZE = 1000;    // 各粮仓大小
    private final int BUFFER_MONITOR_TIME = 5000;    // 对粮仓监控间隔
    private Thread inputBufferMonitor = new InputBufferMonitor();    // 对粮仓进行监控

    public MultipleImportDatas() {
        inputBufferMonitor.start();
    }

    class InputBufferMonitor extends Thread {
        public void run() {
            while (true) {
                try {
                    Thread.sleep(BUFFER_MONITOR_TIME);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                for (Map.Entry<String, List<Map<String, String>>> entity : inputBuffer
                        .entrySet())   // 各粮仓监管
                {
                    String dataType = entity.getKey();
                    concurrentImportDatas(dataType, null, true);    // 线程安全入数据。入库需时间到
                }
            }
        }
    }

    private void resetInputBufferDate(String dateTypeCode)   // 更新清仓时间
    {
        Date date = new Date();
        if (!inputBufferDate.containsKey(dateTypeCode)) {
            inputBufferDate.put(dateTypeCode, date);
        } else {
            inputBufferDate.replace(dateTypeCode, date);
        }
    }

    public void concurrentImportDatas(String dataTypeCode,
                                      List<Map<String, String>> datas, boolean timeUP)    // 线程安全入数据。入库需满仓或时间到
    {
//        DataHBaseSolrBLL dataHBaseSolrBLL = (DataHBaseSolrBLL) SpringRabbitMQSupport
//            .getContext().getBean("dataHBaseSolrBLLImpl");
        if (datas != null && datas.size() >= BUFFER_SIZE)    // 一次入粮多
        {
//            try
//            {
//                dataHBaseSolrBLL.importDataBatch(dataTypeCode, datas);   // 批量入库
//            }
//            catch (DatabaseException | BusinessException e)
//            {
//                e.printStackTrace();
//            }
            return;
        }
//        synchronized (inputBuffer)
        {
            if (!inputBuffer.containsKey(dataTypeCode))     // 无仓
            {
                if (datas != null) {
                    inputBuffer.put(dataTypeCode, datas);   // 建仓
                }
                return;
            }
            if (timeUP)   // 时间到
            {
                Date date = new Date();
                if (((!inputBufferDate.containsKey(dataTypeCode)) || (inputBufferDate
                        .containsKey(dataTypeCode) && date.getTime()
                        - inputBufferDate.get(dataTypeCode).getTime() > BUFFER_MONITOR_TIME))
                        && inputBuffer.get(dataTypeCode).size() > 0) {
                    importInputBufferAndClear(dataTypeCode);   // 清仓
                }
            } else {
                List<Map<String, String>> ldatas = inputBuffer
                        .get(dataTypeCode);
                if (datas.size() + ldatas.size() > BUFFER_SIZE)   // 要满仓
                {
                    importInputBufferAndClear(dataTypeCode);   // 清仓
                }
                ldatas.addAll(datas);    // 未满仓，入仓
            }
        }
    }

    private void importInputBufferAndClear(String dataTypeCode)   // 清仓
    {
//        DataHBaseSolrBLL dataHBaseSolrBLL = (DataHBaseSolrBLL) SpringRabbitMQSupport
//            .getContext().getBean("dataHBaseSolrBLLImpl");
        List<Map<String, String>> ldatas = inputBuffer.get(dataTypeCode);
//        try
//        {
//            dataHBaseSolrBLL.importDataBatch(dataTypeCode, ldatas);   // 批量入库
//        }
//        catch (DatabaseException | BusinessException e)
//        {
//            e.printStackTrace();
//        }
        ldatas.clear();
        resetInputBufferDate(dataTypeCode);   // 更新清仓时间
    }
}