package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.beust.jcommander.JCommander;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.primitives.Longs;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.util.CollectionUtils;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.Metrics;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Commons;
import org.tron.common.utils.StringUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.services.RpcApiService;
import org.tron.core.services.http.FullNodeHttpApiService;
import org.tron.core.services.interfaceJsonRpcOnPBFT.JsonRpcServiceOnPBFT;
import org.tron.core.services.interfaceJsonRpcOnSolidity.JsonRpcServiceOnSolidity;
import org.tron.core.services.interfaceOnPBFT.RpcApiServiceOnPBFT;
import org.tron.core.services.interfaceOnPBFT.http.PBFT.HttpApiOnPBFTService;
import org.tron.core.services.interfaceOnSolidity.RpcApiServiceOnSolidity;
import org.tron.core.services.interfaceOnSolidity.http.solidity.HttpApiOnSolidityService;
import org.tron.core.services.jsonrpc.FullNodeJsonRpcHttpService;
import org.tron.protos.Protocol;

import static org.tron.protos.Protocol.TransactionInfo.code.SUCESS;

@Slf4j(topic = "app")
public class FullNode {

  public static void load(String path) {
    try {
      File file = new File(path);
      if (!file.exists() || !file.isFile() || !file.canRead()) {
        return;
      }
      LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(lc);
      lc.reset();
      configurator.doConfigure(file);
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
  }

  private static final Map<String, Boolean> compareMap = new HashMap<>();

  /** Start the FullNode. */
  public static void main(String[] args) {
    logger.info("Full node running.");
    Args.setParam(args, Constant.TESTNET_CONF);
    CommonParameter parameter = Args.getInstance();

    load(parameter.getLogbackPath());

    if (parameter.isHelp()) {
      JCommander jCommander = JCommander.newBuilder().addObject(Args.PARAMETER).build();
      jCommander.parse(args);
      Args.printHelp(jCommander);
      return;
    }

    if (Args.getInstance().isDebug()) {
      logger.info("in debug mode, it won't check energy time");
    } else {
      logger.info("not in debug mode, it will check energy time");
    }

    // init metrics first
    Metrics.init();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context = new TronApplicationContext(beanFactory);
    context.register(DefaultConfig.class);
    context.refresh();
    Application appT = ApplicationFactory.create(context);
    context.registerShutdownHook();

    // grpc api server
    RpcApiService rpcApiService = context.getBean(RpcApiService.class);
    appT.addService(rpcApiService);

    // http api server
    FullNodeHttpApiService httpApiService = context.getBean(FullNodeHttpApiService.class);
    if (CommonParameter.getInstance().fullNodeHttpEnable) {
      appT.addService(httpApiService);
    }

    // JSON-RPC http server
    if (CommonParameter.getInstance().jsonRpcHttpFullNodeEnable) {
      FullNodeJsonRpcHttpService jsonRpcHttpService =
          context.getBean(FullNodeJsonRpcHttpService.class);
      appT.addService(jsonRpcHttpService);
    }

    // full node and solidity node fuse together
    // provide solidity rpc and http server on the full node.
    RpcApiServiceOnSolidity rpcApiServiceOnSolidity =
        context.getBean(RpcApiServiceOnSolidity.class);
    appT.addService(rpcApiServiceOnSolidity);
    HttpApiOnSolidityService httpApiOnSolidityService =
        context.getBean(HttpApiOnSolidityService.class);
    if (CommonParameter.getInstance().solidityNodeHttpEnable) {
      appT.addService(httpApiOnSolidityService);
    }

    // JSON-RPC on solidity
    if (CommonParameter.getInstance().jsonRpcHttpSolidityNodeEnable) {
      JsonRpcServiceOnSolidity jsonRpcServiceOnSolidity =
          context.getBean(JsonRpcServiceOnSolidity.class);
      appT.addService(jsonRpcServiceOnSolidity);
    }

    // PBFT API (HTTP and GRPC)
    RpcApiServiceOnPBFT rpcApiServiceOnPBFT = context.getBean(RpcApiServiceOnPBFT.class);
    appT.addService(rpcApiServiceOnPBFT);
    HttpApiOnPBFTService httpApiOnPBFTService = context.getBean(HttpApiOnPBFTService.class);
    appT.addService(httpApiOnPBFTService);

    // JSON-RPC on PBFT
    if (CommonParameter.getInstance().jsonRpcHttpPBFTNodeEnable) {
      JsonRpcServiceOnPBFT jsonRpcServiceOnPBFT = context.getBean(JsonRpcServiceOnPBFT.class);
      appT.addService(jsonRpcServiceOnPBFT);
    }
    appT.startup();
    appT.blockUntilShutdown();

    long latestBlock = ChainBaseManager.getInstance().getHeadBlockNum();

    // 发射前
    byte[] TOKEN_PURCHASE_TOPIC =
        Hex.decode("63abb62535c21a5d221cf9c15994097b8880cc986d82faf80f57382b998dbae5");
    byte[] TOKEN_SOLD_TOPIC =
        Hex.decode("9387a595ac4be9038bbb9751abad8baa3dcf219dd9e19abb81552bd521fe3546");
    byte[] TRX_RECEIVED =
        Hex.decode("1bab02886c659969cbb004cc17dc19be19f193323a306e26c669bedb29c651f7");
    String PUMP_BUY_METHOD_1 = "1cc2c911";
    String PUMP_BUY_METHOD_2 = "2f70d762";
    String PUMP_SELL_METHOD = "d19aa2b9";
    byte[] SUNPUMP_LAUNCH = Hex.decode("41c22dd1b7bc7574e94563c8282f64b065bc07b2fa");
    BigDecimal TRX_DIVISOR = new BigDecimal("1000000");
    BigDecimal TOKEN_DIVISOR = new BigDecimal("1000000000000000000");

    // 发射后
    String SWAP_BUY_METHOD_1 = "fb3bdb41";
    String SWAP_BUY_METHOD_2 = "7ff36ab5";
    String SWAP_BUY_METHOD_3 = "b6f9de95";
    String SWAP_SELL_METHOD_1 = "18cbafe5";
    String SWAP_SELL_METHOD_2 = "4a25d94a";
    String SWAP_SELL_METHOD_3 = "791ac947";
    byte[] SWAP_ROUTER = Hex.decode("41fF7155b5df8008fbF3834922B2D52430b27874f5");
    byte[] TRANSFER_TOPIC =
        Hex.decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    byte[] SWAP_TOPIC =
        Hex.decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");
    byte[] WTRX_HEX = Hex.decode("891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18");
    String WTRX = "891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
    BigDecimal buffer = BigDecimal.ONE;

    try {

      BufferedReader reader = new BufferedReader(new FileReader("paddrs.txt"));
      Set<String> paddrs = new HashSet<>();
      String line;
      while ((line = reader.readLine()) != null) {
        paddrs.add(Hex.toHexString(Commons.decodeFromBase58Check(line)));
      }
      BufferedReader sreader = new BufferedReader(new FileReader("saddrs.txt"));
      Set<String> saddrs = new HashSet<>();
      while ((line = sreader.readLine()) != null) {
        saddrs.add(Hex.toHexString(Commons.decodeFromBase58Check(line)));
      }

      //      long startBlock =
      //          Math.max(ChainBaseManager.getChainBaseManager().getLowestBlockNum(), latestBlock -
      // 10000);
      //      long endBlock = latestBlock - 1;
      //      long recentBlock = latestBlock - 3000;
      // todo
      long startBlock = 64184959;
      long recentBlock = 64689819;
      long endBlock = 65092826;
      logger.info(
          "Start To Local Test at {}!!! paddr size {}, saddr size {}",
          startBlock,
          paddrs.size(),
          saddrs.size());
      long logBlock = startBlock;
      long pSumTxCount = 0;
      long pSumBuyCount = 0;
      long sSumTxCount = 0;
      long sSumBuyCount = 0;
      long pSumTxCountrecent = 0;
      long pSumBuyCountrecent = 0;
      long sSumTxCountrecent = 0;
      long sSumBuyCountrecent = 0;

      //      Map<String, Map<String, BuyAndSellRecordV2>> pumpLastBlockBuyAndSellMap = new
      // HashMap<>();
      //      Map<String, Map<String, BuyAndSellRecordV2>> swapLastBlockBuyAndSellMap = new
      // HashMap<>();
      //      Map<String, AddressAllInfo> swapAddressAllInfoMap = new HashMap<>();
      //      Map<String, AddressAllInfo> swapRecentAddressAllInfoMap = new HashMap<>();
      //      Map<String, AddressAllInfo> pumpAddressAllInfoMap = new HashMap<>();
      //      Map<String, AddressAllInfo> pumpRecentAddressAllInfoMap = new HashMap<>();
      // 记录区
      // 总记录，最后出数据的结构
      Map<String, AddrAllInfoRecord> pumpAddrInfoRecordMap = new HashMap<>();
      Map<String, AddrAllInfoRecord> recentpumpAddrInfoRecordMap = new HashMap<>();
      Map<String, AddrAllInfoRecord> swapAddrInfoRecordMap = new HashMap<>();
      Map<String, AddrAllInfoRecord> recentswapAddrInfoRecordMap = new HashMap<>();
      // 连续块记录，每个块后更新
      Map<String, AddrContinusRecord> pumpContinusRecordMap = new HashMap<>();
      Map<String, AddrContinusRecord> recentpumpContinusRecordMap = new HashMap<>();
      Map<String, AddrContinusRecord> swapContinusRecordMap = new HashMap<>();
      Map<String, AddrContinusRecord> recentswapContinusRecordMap = new HashMap<>();

      Map<String, String> pairToTokenMap = populateMap();
      DBIterator retIterator =
          (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
      retIterator.seek(ByteArray.fromLong(startBlock));
      DBIterator blockIterator =
          (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
      blockIterator.seek(ByteArray.fromLong(startBlock));
      while (retIterator.hasNext() && blockIterator.hasNext()) {
        Map.Entry<byte[], byte[]> retEntry = retIterator.next();
        Map.Entry<byte[], byte[]> blockEntry = blockIterator.next();

        byte[] key = retEntry.getKey();
        long blockNum = Longs.fromByteArray(key);
        long blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
        while (blockNum != blockStoreNum) {
          blockEntry = blockIterator.next();
          blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
        }
        if (blockNum > endBlock) {
          break;
        }

        byte[] value = retEntry.getValue();
        TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
        BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());

        long timestamp = transactionRetCapsule.getInstance().getBlockTimeStamp();

        Map<String, String> txCallerMap = new HashMap<>();
        for (TransactionCapsule tx : blockCapsule.getTransactions()) {
          txCallerMap.put(tx.getTransactionId().toString(), Hex.toHexString(tx.getOwnerAddress()));
        }

        for (Protocol.TransactionInfo transactionInfo :
            transactionRetCapsule.getInstance().getTransactioninfoList()) {
          // 只遍历成功交易
          if (!transactionInfo.getResult().equals(SUCESS)) {
            continue;
          }
          byte[] txId = transactionInfo.getId().toByteArray();
          String caller = get41Addr(txCallerMap.get(Hex.toHexString(txId)));
          byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();

          if (Arrays.equals(contractAddress, SWAP_ROUTER)) {
            // Swap tx
            sSumTxCount++;
            if (blockNum >= recentBlock) {
              sSumTxCountrecent++;
            }
            // todo
            if (!saddrs.contains(caller)) {
              continue;
            }
            for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
              if (!Arrays.equals(log.getTopics(0).toByteArray(), SWAP_TOPIC)) {
                continue;
              }
              // Swap topic
              String logData = Hex.toHexString(log.getData().toByteArray());
              BigInteger amount0In = new BigInteger(logData.substring(0, 64), 16);
              BigInteger amount1In = new BigInteger(logData.substring(64, 128), 16);
              BigInteger amount0Out = new BigInteger(logData.substring(128, 192), 16);
              BigInteger amount1Out = new BigInteger(logData.substring(192, 256), 16);

              String pair = Hex.toHexString(log.getAddress().toByteArray());
              String token = pairToTokenMap.get(pair);
              boolean tokenNull = token == null;
              if (tokenNull) {
                for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
                  if (Arrays.equals(TRANSFER_TOPIC, log2.getTopics(0).toByteArray())
                      && !Arrays.equals(log2.getAddress().toByteArray(), WTRX_HEX)) {
                    token = Hex.toHexString(log2.getAddress().toByteArray());
                    break;
                  }
                }
              }
              boolean smaller = smallerToWtrx(token, WTRX);
              token = get41Addr(token);

              boolean isBuy =
                  ((smaller && amount0Out.compareTo(BigInteger.ZERO) > 0)
                      || (!smaller && amount1Out.compareTo(BigInteger.ZERO) > 0));

              BigDecimal trxAmount;
              BigDecimal tokenAmount;
              if (isBuy) {
                sSumBuyCount++;
                if (blockNum >= recentBlock) {
                  sSumBuyCountrecent++;
                }
                if (smaller) {
                  trxAmount =
                      new BigDecimal(amount1In).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                  tokenAmount =
                      new BigDecimal(amount0Out).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                } else {
                  trxAmount =
                      new BigDecimal(amount0In).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                  tokenAmount =
                      new BigDecimal(amount1Out).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                }
              } else {
                if (smaller) {
                  trxAmount =
                      new BigDecimal(amount1Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                  tokenAmount =
                      new BigDecimal(amount0In).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                } else {
                  trxAmount =
                      new BigDecimal(amount0Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                  tokenAmount =
                      new BigDecimal(amount1In).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                }
              }
              // 这里只记录
              AddrContinusRecord addrContinusRecord =
                  swapContinusRecordMap.getOrDefault(caller, new AddrContinusRecord(caller));
              addrContinusRecord.addRecord(blockNum, token, isBuy, tokenAmount, trxAmount);
              swapContinusRecordMap.put(caller, addrContinusRecord);
              if (blockNum >= recentBlock) {
                AddrContinusRecord recentaddrContinusRecord =
                    recentswapContinusRecordMap.getOrDefault(
                        caller, new AddrContinusRecord(caller));
                recentaddrContinusRecord.addRecord(blockNum, token, isBuy, tokenAmount, trxAmount);
                recentswapContinusRecordMap.put(caller, recentaddrContinusRecord);
              }
              // 找到目标log就跳出
              break;
            }
          } else if (Arrays.equals(contractAddress, SUNPUMP_LAUNCH)) {
            pSumTxCount++;
            if (blockNum >= recentBlock) {
              pSumTxCountrecent++;
            }
            for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {

              boolean isBuy = false;
              boolean flag = false;
              if (Arrays.equals(log.getTopics(0).toByteArray(), TOKEN_SOLD_TOPIC)) {
                flag = true;
              } else if (Arrays.equals(log.getTopics(0).toByteArray(), TOKEN_PURCHASE_TOPIC)) {
                flag = true;
                isBuy = true;
              }
              if (!flag) {
                continue;
              }
              if (isBuy) {
                pSumBuyCount++;
                if (blockNum >= recentBlock) {
                  pSumBuyCountrecent++;
                }
              }

              // todo
              if (!paddrs.contains(caller)) {
                continue;
              }
              String token = get41Addr(Hex.toHexString(log.getAddress().toByteArray()));
              for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
                if (Arrays.equals(log2.getTopics(0).toByteArray(), TRANSFER_TOPIC)) {
                  token = get41Addr(Hex.toHexString(log2.getAddress().toByteArray()));
                  break;
                }
              }

              String dataStr = Hex.toHexString(log.getData().toByteArray());
              //              BigDecimal trxAmount =
              //                  new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
              //                      .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              //              BigDecimal feeAmount =
              //                  new BigDecimal(new BigInteger(dataStr.substring(64, 128), 16))
              //                      .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              BigDecimal trxAmount;
              if (isBuy) {
                BigDecimal trxAmount1 =
                    new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                BigDecimal feeAmount1 =
                    new BigDecimal(new BigInteger(dataStr.substring(64, 128), 16))
                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                BigDecimal trxAmount2 = BigDecimal.ZERO;
                for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
                  if (Arrays.equals(log2.getTopics(0).toByteArray(), TRX_RECEIVED)) {
                    trxAmount2 =
                        trxAmount2.add(
                            new BigDecimal(
                                    new BigInteger(
                                        Hex.toHexString(log2.getData().toByteArray()), 16))
                                .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN));
                  }
                }
                trxAmount =
                    trxAmount1.compareTo(trxAmount2) > 0 ? trxAmount1.add(feeAmount1) : trxAmount2;
              } else {
                trxAmount =
                    new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              }

              BigDecimal tokenAmount =
                  new BigDecimal(new BigInteger(dataStr.substring(128, 192), 16))
                      .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);

              // 这里只记录
              AddrContinusRecord addrContinusRecord =
                  pumpContinusRecordMap.getOrDefault(caller, new AddrContinusRecord(caller));
              addrContinusRecord.addRecord(blockNum, token, isBuy, tokenAmount, trxAmount);
              pumpContinusRecordMap.put(caller, addrContinusRecord);
              if (blockNum >= recentBlock) {
                AddrContinusRecord recentaddrContinusRecord =
                    recentpumpContinusRecordMap.getOrDefault(
                        caller, new AddrContinusRecord(caller));
                recentaddrContinusRecord.addRecord(blockNum, token, isBuy, tokenAmount, trxAmount);
                recentpumpContinusRecordMap.put(caller, recentaddrContinusRecord);
              }
              // 找到目标log就跳出
              break;
            }
          }
        }
        // 当前块交易结束, 处理这个块的买卖，并更新记录
        proceeToBlock(swapContinusRecordMap, swapAddrInfoRecordMap, blockNum);
        proceeToBlock(pumpContinusRecordMap, pumpAddrInfoRecordMap, blockNum);
        if (blockNum >= recentBlock) {
          proceeToBlock(recentswapContinusRecordMap, recentswapAddrInfoRecordMap, blockNum);
          proceeToBlock(recentpumpContinusRecordMap, recentpumpAddrInfoRecordMap, blockNum);
        }

        if (blockNum - logBlock >= 10000) {
          logBlock = blockNum;
          logger.info(
              "Sync to block {} timestamp {}, sum p addr {}, s addr {}, p_sum_tx_count {}, s_sum_tx_count {}",
              blockNum,
              timestamp,
              pumpAddrInfoRecordMap.keySet().size(),
              swapAddrInfoRecordMap.keySet().size(),
              pSumTxCount,
              sSumTxCount);
        }
      }

      // 输出结果哦
      PrintWriter pwriter = new PrintWriter("finalresult.txt");
      pwriter.println("SWAP");
      swapAddrInfoRecordMap.forEach(
          (k, v) ->
              pwriter.println(
                  k
                      + " "
                      + v.getSuccessCount()
                      + " "
                      + v.getAllProfit()
                      + " "
                      + v.getLackCount()
                      + " "
                      + v.getAllLack()
                      + " "
                      + v.getTrxOutAmount()));
      pwriter.println("RECENTSWAP");
      recentswapAddrInfoRecordMap.forEach(
          (k, v) ->
              pwriter.println(
                  k
                      + " "
                      + v.getSuccessCount()
                      + " "
                      + v.getAllProfit()
                      + " "
                      + v.getLackCount()
                      + " "
                      + v.getAllLack()
                      + " "
                      + v.getTrxOutAmount()));
      pwriter.println("PUMP");
      pumpAddrInfoRecordMap.forEach(
          (k, v) ->
              pwriter.println(
                  k
                      + " "
                      + v.getSuccessCount()
                      + " "
                      + v.getAllProfit()
                      + " "
                      + v.getLackCount()
                      + " "
                      + v.getAllLack()
                      + " "
                      + v.getTrxOutAmount()));
      pwriter.println("RECENTPUMP");
      recentpumpAddrInfoRecordMap.forEach(
          (k, v) ->
              pwriter.println(
                  k
                      + " "
                      + v.getSuccessCount()
                      + " "
                      + v.getAllProfit()
                      + " "
                      + v.getLackCount()
                      + " "
                      + v.getAllLack()
                      + " "
                      + v.getTrxOutAmount()));

      pwriter.println("TOKEN_REMAINING");
      pwriter.println("SWAPTOKEN");
      swapAddrInfoRecordMap.forEach(
          (k, v) -> {
            pwriter.println(k);
            v.records.forEach(
                (k1, v1) -> {
                  if (v.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
                    pwriter.println(
                        "Token " + k1 + " " + v1.remainingTokenAmount + v1.trxOutAmount);
                  }
                });
          });
      pwriter.println("RECENTSWAPTOKEN");
      recentswapAddrInfoRecordMap.forEach(
          (k, v) -> {
            pwriter.println(k);
            v.records.forEach(
                (k1, v1) -> {
                  if (v.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
                    pwriter.println(
                        "Token " + k1 + " " + v1.remainingTokenAmount + v1.trxOutAmount);
                  }
                });
          });
      pwriter.println("PUMPTOKEN");
      pumpAddrInfoRecordMap.forEach(
          (k, v) -> {
            pwriter.println(k);
            v.records.forEach(
                (k1, v1) -> {
                  if (v.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
                    pwriter.println(
                        "Token " + k1 + " " + v1.remainingTokenAmount + v1.trxOutAmount);
                  }
                });
          });
      pwriter.println("RECENTPUMPTOKEN");
      recentpumpAddrInfoRecordMap.forEach(
          (k, v) -> {
            pwriter.println(k);
            v.records.forEach(
                (k1, v1) -> {
                  if (v.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
                    pwriter.println(
                        "Token " + k1 + " " + v1.remainingTokenAmount + v1.trxOutAmount);
                  }
                });
          });

      pwriter.close();
      logger.info(
          "Final syncing sum p addr {}, s addr {}, p_sum_tx_count {}, p_buy {}, s_sum_tx_count {}, s_buy {}, p_recent_tx_count {}, p_recent_buy {}, s_recent_tx_count {}, s_recent_buy {}",
          pumpAddrInfoRecordMap.keySet().size(),
          swapAddrInfoRecordMap.keySet().size(),
          pSumTxCount,
          pSumBuyCount,
          sSumTxCount,
          sSumBuyCount,
          pSumTxCountrecent,
          pSumBuyCountrecent,
          sSumTxCountrecent,
          sSumBuyCountrecent);

    } catch (Exception e) {
      logger.info("ERROR!!!", e);
    }

    logger.info("END");
  }

  private static void proceeToBlock(
      Map<String, AddrContinusRecord> continusRecordMap,
      Map<String, AddrAllInfoRecord> addrAllInfoRecordMap,
      long blockNum) {
    for (Map.Entry<String, AddrContinusRecord> entry : continusRecordMap.entrySet()) {
      // 每个人
      String addr = entry.getKey();
      AddrAllInfoRecord addrAllInfoRecord =
          addrAllInfoRecordMap.getOrDefault(addr, new AddrAllInfoRecord(addr));

      AddrContinusRecord addrTwoBlockRecord = entry.getValue();
      Map<String, ContinusBlockRecord> thisBlockRecords =
          addrTwoBlockRecord.getRecordsByBlockNum(blockNum);
      Map<String, ContinusBlockRecord> lastBlockRecords =
          addrTwoBlockRecord.getRecordsByBlockNum(blockNum - 1);
      // 理论上 lastblock 记录只剩 买记录
      for (Map.Entry<String, ContinusBlockRecord> tokenEntry : thisBlockRecords.entrySet()) {
        // 每个 token
        String token = tokenEntry.getKey();
        ContinusBlockRecord buySellsThisBlocks = tokenEntry.getValue();
        ContinusBlockRecord buySellsLastBlocks =
            lastBlockRecords.getOrDefault(token, new ContinusBlockRecord());

        // 第一遍，match上的
        for (int i = 0; i < buySellsThisBlocks.records.size(); i++) {
          // caller 每个 token 买卖
          SingleBuySellRecord buySell = buySellsThisBlocks.records.get(i);
          if (buySell.isBuy()) {
            addrAllInfoRecord.addBuy();
          } else {
            // 卖，最近两块匹配
            matchBuySell(
                buySell,
                buySellsLastBlocks.records,
                addrAllInfoRecord,
                token,
                buySellsLastBlocks.records.size());
            if (!buySell.matched) {
              matchBuySell(buySell, buySellsThisBlocks.records, addrAllInfoRecord, token, i);
            }
          }
        }

        // 清空matched
        buySellsThisBlocks.records.removeIf(SingleBuySellRecord::isMatched);
        buySellsLastBlocks.records.removeIf(SingleBuySellRecord::isMatched);

        // 第二遍找没匹配上的
        List<SingleBuySellRecord> sellsThisBlock =
            buySellsThisBlocks.records.stream()
                .filter(sell -> !sell.isBuy)
                .collect(Collectors.toList());
        List<SingleBuySellRecord> buyThisBlock =
            buySellsThisBlocks.records.stream()
                .filter(buy -> buy.isBuy)
                .collect(Collectors.toList());

        // 上一个块全是卖

        // 先平上一个块的帐
        // 想简单一点，如果这两个块剩余总买总卖对的上，就算获利或损失;
        // 如果对不上，则上一个块的买卖去平账，这个块的记到下一个块
        BigDecimal allBuyTokenAmountLastBlock = BigDecimal.ZERO;
        BigDecimal allOutTrxAmountLastBlock = BigDecimal.ZERO;
        for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
          allBuyTokenAmountLastBlock = allBuyTokenAmountLastBlock.add(buy.getTokenAmount());
          allOutTrxAmountLastBlock = allOutTrxAmountLastBlock.add(buy.getTrxAmount());
        }
        BigDecimal allBuyTokenAmountThisBlock = BigDecimal.ZERO;
        BigDecimal allOutTrxAmountThisBlock = BigDecimal.ZERO;
        for (SingleBuySellRecord buy : buyThisBlock) {
          allBuyTokenAmountThisBlock = allBuyTokenAmountThisBlock.add(buy.getTokenAmount());
          allOutTrxAmountThisBlock = allOutTrxAmountThisBlock.add(buy.getTrxAmount());
        }
        BigDecimal allSellTokenAmount = BigDecimal.ZERO;
        BigDecimal allGetTrxAmount = BigDecimal.ZERO;
        // 算总金额
        for (SingleBuySellRecord sell : sellsThisBlock) {
          allSellTokenAmount = allSellTokenAmount.add(sell.getTokenAmount());
          allGetTrxAmount = allGetTrxAmount.add(sell.getTrxAmount());
        }

        TokenAllInfoRecord tokenAllInfoRecord = addrAllInfoRecord.getTokenAllInfoRecord(token);
        if (allSellTokenAmount.compareTo(BigDecimal.ZERO) > 0) {
          if (isMatch(
              allSellTokenAmount.add(buySellsLastBlocks.remainingSellTokenAmount),
              allBuyTokenAmountLastBlock.add(allBuyTokenAmountThisBlock))) {
            addrAllInfoRecord.addTokenRecord(
                token,
                allGetTrxAmount
                    .add(buySellsLastBlocks.remainingGetTrxAmount)
                    .subtract(allOutTrxAmountLastBlock)
                    .subtract(allOutTrxAmountThisBlock));
            // 这两个块全都匹配上了，不留记录
            buySellsThisBlocks.clearAll();
            buySellsLastBlocks.clearAll();
          } else {
            // 没匹配上，则 上一个块的remaining 和 上一个块的买去平账；本块的记到下一个块
            BigDecimal actualSellTokenAmount =
                tokenAllInfoRecord.remainingTokenAmount.compareTo(allSellTokenAmount) < 0
                    ? tokenAllInfoRecord.remainingTokenAmount
                    : allSellTokenAmount;
            BigDecimal actualGetTrxAmount =
                actualSellTokenAmount
                    .multiply(allGetTrxAmount)
                    .divide(allSellTokenAmount, 6, RoundingMode.HALF_EVEN);
            // 把这个块没匹配上的卖去平账
            tokenAllInfoRecord.removeRemaining(actualSellTokenAmount, actualGetTrxAmount);
          }
        }
        // 把上个块没匹配上的买累计到帐
        tokenAllInfoRecord.addRemaining(allBuyTokenAmountLastBlock, allOutTrxAmountLastBlock);
        addrAllInfoRecordMap.put(addr, addrAllInfoRecord);

        // 本块记录移到上一个块
        buySellsThisBlocks.updateRecords(buyThisBlock, allSellTokenAmount, allGetTrxAmount);
        // update
        thisBlockRecords.put(token, buySellsThisBlocks);
        addrTwoBlockRecord.updateRecordsByBlockNum(blockNum, thisBlockRecords);
        continusRecordMap.put(addr, addrTwoBlockRecord);
      }
    }
    // 删除1个块之前记录
    for (Map.Entry<String, AddrContinusRecord> entry : continusRecordMap.entrySet()) {
      entry.getValue().removeRecordByBlockNum(blockNum - 1);
      if (entry.getValue().records.isEmpty()) {
        continusRecordMap.remove(entry.getKey());
      }
    }
  }

  private static void matchBuySell(
      SingleBuySellRecord sellRecord,
      List<SingleBuySellRecord> buySells,
      AddrAllInfoRecord record,
      String token,
      int endIndex) {

    for (int i = 0; i < Math.min(endIndex, buySells.size()); i++) {
      SingleBuySellRecord toMatch = buySells.get(i);
      if (toMatch.isBuy && isMatch(toMatch.tokenAmount, sellRecord.tokenAmount)) {
        // 匹配上，计算获利和损失
        BigDecimal getTrx = sellRecord.trxAmount.subtract(toMatch.trxAmount);
        record.addTokenRecord(token, getTrx);
        toMatch.match();
        sellRecord.match();
        break;
      }
    }
  }

  private static String get41Addr(String hexAddr) {
    if (!hexAddr.startsWith("41")) {
      return "41" + hexAddr;
    }
    return hexAddr;
  }

  private static Map<String, String> populateMap() throws IOException {
    // token pair map
    File file = new File("test.txt");
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    Map<String, String> map = new HashMap<>();
    while ((line = reader.readLine()) != null) {
      String[] strings = line.split(" ");
      String token = strings[0];
      String pair = strings[1];
      long blockNum = Long.parseLong(strings[2]);
      long timestamp = Long.parseLong(strings[3]);

      map.put(
          Hex.toHexString(Commons.decodeFromBase58Check(pair)).substring(2),
          Hex.toHexString(Commons.decodeFromBase58Check(token)).substring(2));
    }
    return map;
  }

  private static boolean smallerToWtrx(String token, String wtrx) {
    return compareMap.computeIfAbsent(token, t -> token.compareTo(wtrx) < 0);
  }

  // 结构体
  @AllArgsConstructor
  @Getter
  private static class SingleBuySellRecord {
    boolean isBuy;
    BigDecimal tokenAmount;
    BigDecimal trxAmount;
    long blockNum;
    boolean matched;

    private SingleBuySellRecord(long blockNum) {
      tokenAmount = BigDecimal.ZERO;
      trxAmount = BigDecimal.ZERO;
      this.blockNum = blockNum;
      matched = false;
    }

    private BigDecimal getTokenPrice() {
      return trxAmount.divide(tokenAmount, 18, RoundingMode.HALF_UP);
    }

    private void match() {
      matched = true;
    }
  }

  @AllArgsConstructor
  @Getter
  private static class ContinusBlockRecord {
    List<SingleBuySellRecord> records;
    BigDecimal remainingSellTokenAmount;
    BigDecimal remainingGetTrxAmount;

    private ContinusBlockRecord() {
      records = new ArrayList<>();
      remainingSellTokenAmount = BigDecimal.ZERO;
      remainingGetTrxAmount = BigDecimal.ZERO;
    }

    private void addRecord(SingleBuySellRecord singleBuySellRecord) {
      records.add(singleBuySellRecord);
    }

    private void clearAll() {
      records.clear();
      remainingSellTokenAmount = BigDecimal.ZERO;
      remainingGetTrxAmount = BigDecimal.ZERO;
    }

    private void updateRecords(
        List<SingleBuySellRecord> newRecords, BigDecimal tokenAmount, BigDecimal trxAmount) {
      records.clear();
      records.addAll(newRecords);
      remainingSellTokenAmount = tokenAmount;
      remainingGetTrxAmount = trxAmount;
    }
  }

  @AllArgsConstructor
  @Getter
  private static class AddrContinusRecord {
    String caller;
    Map<Long, Map<String, ContinusBlockRecord>> records; // <token, ContinusBlockRecord>

    private AddrContinusRecord(String caller) {
      this.caller = caller;
      records = new HashMap<>();
    }

    private void addRecord(
        long blockNum, String token, boolean isBuy, BigDecimal tokenAmount, BigDecimal trxAmount) {
      Map<String, ContinusBlockRecord> tokenContinusRecord =
          records.getOrDefault(blockNum, new HashMap<>());

      ContinusBlockRecord record =
          tokenContinusRecord.getOrDefault(token, new ContinusBlockRecord());
      SingleBuySellRecord singleBuySellRecord =
          new SingleBuySellRecord(isBuy, tokenAmount, trxAmount, blockNum, false);
      record.addRecord(singleBuySellRecord);
      tokenContinusRecord.put(token, record);
      records.put(blockNum, tokenContinusRecord);
    }

    private Map<String, ContinusBlockRecord> getRecordsByBlockNum(long thisBlockNum) {
      return records.getOrDefault(thisBlockNum, new HashMap<>());
    }

    private void updateRecordsByBlockNum(
        long thisBlockNum, Map<String, ContinusBlockRecord> record) {
      records.put(thisBlockNum, record);
    }

    private void removeRecordByBlockNum(long blockNum) {
      records.remove(blockNum);
    }
  }

  @AllArgsConstructor
  @Getter
  private static class TokenAllInfoRecord {
    String token;
    BigDecimal profit;
    BigDecimal lack;
    long successCount;
    long lackCount;
    BigDecimal remainingTokenAmount;
    BigDecimal trxOutAmount;

    private TokenAllInfoRecord(String token) {
      this.token = token;
      profit = BigDecimal.ZERO;
      lack = BigDecimal.ZERO;
      successCount = 0;
      lackCount = 0;
      remainingTokenAmount = BigDecimal.ZERO;
      trxOutAmount = BigDecimal.ZERO;
    }

    private void addTrxDiff(BigDecimal trxDiff) {
      if (trxDiff.compareTo(BigDecimal.ZERO) > 0) {
        profit = profit.add(trxDiff);
        successCount++;
      } else {
        lack = lack.add(trxDiff);
        lackCount++;
      }
    }

    private void removeRemaining(BigDecimal tokenAmount, BigDecimal trxGetAmount) {
      remainingTokenAmount = remainingTokenAmount.subtract(tokenAmount);
      this.trxOutAmount = this.trxOutAmount.subtract(trxGetAmount);
    }

    private void addRemaining(BigDecimal tokenAmount, BigDecimal trxOutAmount) {
      remainingTokenAmount = remainingTokenAmount.add(tokenAmount);
      this.trxOutAmount = this.trxOutAmount.add(trxOutAmount);
    }
  }

  @AllArgsConstructor
  @Getter
  private static class AddrAllInfoRecord {
    String caller;
    long buyCount;
    Map<String, TokenAllInfoRecord> records; // <token, TokenAllInfoRecord>

    private AddrAllInfoRecord(String caller) {
      this.caller = caller;
      buyCount = 0;
      records = new HashMap<>();
    }

    private void addBuy() {
      buyCount++;
    }

    private void addTokenRecord(String token, BigDecimal trxDiff) {

      TokenAllInfoRecord tokenAllInfoRecord =
          records.getOrDefault(token, new TokenAllInfoRecord(token));
      tokenAllInfoRecord.addTrxDiff(trxDiff);
      records.put(token, tokenAllInfoRecord);
    }

    private TokenAllInfoRecord getTokenAllInfoRecord(String token) {
      return records.getOrDefault(token, new TokenAllInfoRecord(token));
    }

    private BigDecimal getAllProfit() {
      return records.values().stream()
          .map(TokenAllInfoRecord::getProfit)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getAllLack() {
      return records.values().stream()
          .map(TokenAllInfoRecord::getLack)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private long getSuccessCount() {
      return records.values().stream().mapToLong(TokenAllInfoRecord::getSuccessCount).sum();
    }

    private long getLackCount() {
      return records.values().stream().mapToLong(TokenAllInfoRecord::getLackCount).sum();
    }

    private BigDecimal getRemainingTokenAmount() {
      return records.values().stream()
          .map(TokenAllInfoRecord::getRemainingTokenAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getTrxOutAmount() {
      return records.values().stream()
          .map(TokenAllInfoRecord::getTrxOutAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
  }

  //  @AllArgsConstructor
  //  @Getter
  //  private static class BuyAndSellRecordV2 {
  //
  //    BigDecimal tokenInAmountThisBlock;
  //    BigDecimal trxOutAmountThisBlock;
  //    BigDecimal tokenOutAmountThisBlock;
  //    BigDecimal trxInAmountThisBlock;
  //    BigDecimal tokenInAmountLastBlock;
  //    BigDecimal trxOutAmountLastBlock;
  //    BigDecimal tokenOutAmountLastBlock;
  //    BigDecimal trxInAmountLastBlock;
  //    long successCount;
  //    BigDecimal recordProfit;
  //
  //    long blockNum;
  //
  //    private BuyAndSellRecordV2(long blockNum) {
  //      tokenInAmountThisBlock = BigDecimal.ZERO;
  //      trxOutAmountThisBlock = BigDecimal.ZERO;
  //      tokenOutAmountThisBlock = BigDecimal.ZERO;
  //      trxInAmountThisBlock = BigDecimal.ZERO;
  //      tokenInAmountLastBlock = BigDecimal.ZERO;
  //      trxOutAmountLastBlock = BigDecimal.ZERO;
  //      tokenOutAmountLastBlock = BigDecimal.ZERO;
  //      trxInAmountLastBlock = BigDecimal.ZERO;
  //      successCount = 0;
  //      recordProfit = BigDecimal.ZERO;
  //      this.blockNum = blockNum;
  //    }
  //
  //    private BuyAndSellRecordV2(
  //        long blockNum,
  //        BigDecimal tokenInAmountLastBlock,
  //        BigDecimal trxOutAmountLastBlock,
  //        BigDecimal tokenOutAmountLastBlock,
  //        BigDecimal trxInAmountLastBlock) {
  //      tokenInAmountThisBlock = BigDecimal.ZERO;
  //      trxOutAmountThisBlock = BigDecimal.ZERO;
  //      tokenOutAmountThisBlock = BigDecimal.ZERO;
  //      trxInAmountThisBlock = BigDecimal.ZERO;
  //      this.tokenInAmountLastBlock = tokenInAmountLastBlock;
  //      this.trxOutAmountLastBlock = trxOutAmountLastBlock;
  //      this.tokenOutAmountLastBlock = tokenOutAmountLastBlock;
  //      this.trxInAmountLastBlock = trxInAmountLastBlock;
  //      successCount = 0;
  //      recordProfit = BigDecimal.ZERO;
  //      this.blockNum = blockNum;
  //    }
  //
  //    private void addBuy(BigDecimal tokenAmount, BigDecimal trxSellAmount) {
  //      tokenInAmountThisBlock = tokenInAmountThisBlock.add(tokenAmount);
  //      this.trxOutAmountThisBlock = this.trxOutAmountThisBlock.add(trxSellAmount);
  //    }
  //
  //    private BigDecimal getTrxByToken(BigDecimal tokenAmount) {
  //      return (this.trxOutAmountThisBlock.add(this.trxOutAmountLastBlock))
  //          .multiply(tokenAmount)
  //          .divide(
  //              (this.tokenInAmountThisBlock.add(this.tokenInAmountLastBlock)),
  //              6,
  //              RoundingMode.HALF_EVEN);
  //    }
  //
  //    private void addSell(BigDecimal tokenAmount, BigDecimal trxBuyAmount, BigDecimal profit) {
  //      tokenOutAmountThisBlock = tokenOutAmountThisBlock.add(tokenAmount);
  //      this.trxInAmountThisBlock = this.trxInAmountThisBlock.add(trxBuyAmount);
  //
  //      if (profit.compareTo(BigDecimal.ZERO) > 0) {
  //        successCount++;
  //        recordProfit = recordProfit.add(profit);
  //      }
  //    }
  //
  //    private boolean hasBuyThisBlock() {
  //      return tokenInAmountThisBlock.compareTo(BigDecimal.ZERO) > 0;
  //    }
  //
  //    private boolean hasSellThisBlock() {
  //      return tokenOutAmountThisBlock.compareTo(BigDecimal.ZERO) > 0;
  //    }
  //
  //    private BigDecimal buyAmountToCoverThisBlock() {
  //      return tokenInAmountThisBlock.subtract(tokenOutAmountThisBlock);
  //    }
  //
  //    private BigDecimal buyAmountToCoverLastBlock() {
  //      return tokenInAmountLastBlock.subtract(tokenOutAmountLastBlock);
  //    }
  //
  //    private BigDecimal remainingInTokenAmount() {
  //      return tokenInAmountThisBlock
  //          .add(tokenInAmountLastBlock)
  //          .subtract(tokenOutAmountThisBlock)
  //          .subtract(tokenOutAmountLastBlock);
  //    }
  //
  //    private BigDecimal mevTrxProfitAmount() {
  //      return trxInAmountThisBlock
  //          .add(trxInAmountLastBlock)
  //          .subtract(trxOutAmountThisBlock)
  //          .subtract(trxOutAmountLastBlock);
  //    }
  //
  //    private BigDecimal trxOutAmountToCover() {
  //      return trxOutAmountThisBlock
  //          .add(trxOutAmountLastBlock)
  //          .subtract(trxInAmountThisBlock)
  //          .subtract(trxInAmountLastBlock);
  //    }
  //  }
  //
  //  @AllArgsConstructor
  //  private static class AddressAllInfo {
  //    private long buyTokenCount;
  //    private long sellTokenCount;
  //    private long successSellCount;
  //    private BigDecimal trxOut;
  //    private BigDecimal trxIn;
  //    private BigDecimal originTrxIn;
  //    private BigDecimal profit;
  //    private BigDecimal lack;
  //
  //    private AddressAllInfo() {
  //      buyTokenCount = 0;
  //      sellTokenCount = 0;
  //      successSellCount = 0;
  //      trxOut = BigDecimal.ZERO;
  //      trxIn = BigDecimal.ZERO;
  //      originTrxIn = BigDecimal.ZERO;
  //      profit = BigDecimal.ZERO;
  //      lack = BigDecimal.ZERO;
  //    }
  //  }

  private static boolean isMatch(BigDecimal first, BigDecimal second) {
    BigDecimal difference = first.subtract(second).abs(); // 计算差的绝对值
    BigDecimal threshold = new BigDecimal("1"); // 设置阈值为 1

    // 比较绝对值是否小于 1
    return difference.compareTo(threshold) <= 0;
  }
}
