package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.Metrics;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.CollectionUtils;
import org.tron.common.utils.Commons;
import org.tron.common.utils.StringUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.exception.BadItemException;
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
import org.tron.protos.contract.SmartContractOuterClass;

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

  private static final Set<String> pumpToRecordSrAddrs =
      new HashSet<>(
          Arrays.asList(
              get41Addr(
                  Hex.toHexString(
                      Commons.decodeFromBase58Check("TEmoTK4PyjJVZhqwzkNfGUsTuhbYGvpk4J"))),
              get41Addr(
                  Hex.toHexString(
                      Commons.decodeFromBase58Check("THKX6VFVy4QwA3yLfgPLJKCc6y7ViDTsZ4")))));
  private static final Set<String> swapToRecordSrAddrs =
      new HashSet<>(
          Arrays.asList(
              get41Addr(
                  Hex.toHexString(
                      Commons.decodeFromBase58Check("TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7"))),
              get41Addr(
                  Hex.toHexString(
                      Commons.decodeFromBase58Check("TB3JcyXpvv3Rz1X3Qcg1Y4nfBwjkHJduyH")))));

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
    //                appT.startup();
    //                appT.blockUntilShutdown();

    //    if (true) {
    //      syncEnergy();
    //      return;
    //    }

    try {
      String targetAddress = "41987c0191a1A098Ffc9addC9C65d2c3d028B10CA3".toLowerCase();
      //      String targetAddress = "4135EF67a96a4f28900fe58D3c2e6703A542d119A1".toLowerCase();

      //      BufferedReader reader = new BufferedReader(new FileReader("paddrs.txt"));
      Set<String> paddrs = new HashSet<>();
      //      String line;
      //      while ((line = reader.readLine()) != null) {
      //        paddrs.add(Hex.toHexString(Commons.decodeFromBase58Check(line)));
      //      }
      //      BufferedReader sreader = new BufferedReader(new FileReader("saddrs.txt"));
      Set<String> saddrs = new HashSet<>();
      saddrs.add(targetAddress);
      //      while ((line = sreader.readLine()) != null) {
      //        saddrs.add(Hex.toHexString(Commons.decodeFromBase58Check(line)));
      //      }
      long startBlock = 65121619;
      //      long startBlock = 64689819;
      long recentBlock = 65323160;
      long endBlock = 65524702;
      //       2024-09-19 11:00:00 ~ 2024-09-25 21:00:00
      //      syncMevStat(65355548, 65553494, 65553494, paddrs, saddrs, targetAddress, "null");
      // 2024-09-19 8:00:00 ~ 2024-09-27 8:00:00
      syncMevStat(65351950, 65582286, 65582286, paddrs, saddrs, targetAddress, "null");
      //      syncMevStat(
      //          ChainBaseManager.getInstance().getHeadBlockNum() - 10000,
      //          65540296,
      //          65540296,
      //          paddrs,
      //          saddrs);
      //
      //      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      //      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      //      // sync day stat
      //      long startTimestamp = 1726012800000L;
      //      long endTimestamp = 1727308800000L;
      //      long endBlockLastDay = 0;
      //      for (long timestmap = startTimestamp;
      //          timestmap < endTimestamp;
      //          timestmap += 1000 * 60 * 60 * 24) {
      //        long curStartBlock =
      //            endBlockLastDay == 0 ? getBlockByTimestamp(timestmap) + 1 : endBlockLastDay + 1;
      //        long curEndBlock = getBlockByTimestamp(timestmap + 1000 * 60 * 60 * 24);
      //        endBlockLastDay = curEndBlock;
      //
      //        syncMevStat(
      //            curStartBlock,
      //            curEndBlock,
      //            curEndBlock,
      //            paddrs,
      //            saddrs,
      //            targetAddress,
      //            dateFormat.format(timestmap));
      //      }
    } catch (Exception e) {
      logger.info("Total Error!!!!", e);
    }

    logger.info("Total End!!!!");
  }

  // 发射前
  private static byte[] TOKEN_PURCHASE_TOPIC =
      Hex.decode("63abb62535c21a5d221cf9c15994097b8880cc986d82faf80f57382b998dbae5");
  private static byte[] TOKEN_SOLD_TOPIC =
      Hex.decode("9387a595ac4be9038bbb9751abad8baa3dcf219dd9e19abb81552bd521fe3546");
  private static byte[] TRX_RECEIVED =
      Hex.decode("1bab02886c659969cbb004cc17dc19be19f193323a306e26c669bedb29c651f7");
  private static String PUMP_BUY_METHOD_1 = "1cc2c911";
  private static String PUMP_BUY_METHOD_2 = "2f70d762";
  private static String PUMP_SELL_METHOD = "d19aa2b9";
  private static byte[] SUNPUMP_LAUNCH = Hex.decode("41c22dd1b7bc7574e94563c8282f64b065bc07b2fa");
  private static BigDecimal TRX_DIVISOR = new BigDecimal("1000000");
  private static BigDecimal TOKEN_DIVISOR = new BigDecimal("1000000000000000000");

  // 发射后
  private static String SWAP_BUY_METHOD_1 = "fb3bdb41"; // swapETHForExactTokens
  private static String SWAP_BUY_METHOD_2 = "7ff36ab5";
  private static String SWAP_BUY_METHOD_3 =
      "b6f9de95"; // swapExactETHForTokensSupportingFeeOnTransferTokens
  private static String SWAP_SELL_METHOD_1 = "18cbafe5";
  private static String SWAP_SELL_METHOD_2 = "4a25d94a"; // swapTokensForExactETH
  private static String SWAP_SELL_METHOD_3 =
      "791ac947"; // swapExactTokensForETHSupportingFeeOnTransferTokens
  private static String SWAP_METHOD = "38ed1739";
  private static byte[] SWAP_ROUTER = Hex.decode("41fF7155b5df8008fbF3834922B2D52430b27874f5");
  private static byte[] SUNSWAP_ROUTER = Hex.decode("41e95812D8D5B5412D2b9F3A4D5a87CA15C5c51F33");
  private static byte[] TRANSFER_TOPIC =
      Hex.decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
  private static byte[] SWAP_TOPIC =
      Hex.decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");
  private static byte[] WTRX_HEX = Hex.decode("891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18");
  private static String WTRX = "891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
  private static String WTRX41 = "41891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
  private static String USDT = "a614f803B6FD780986A42c78Ec9c7f77e6DeD13C".toLowerCase();
  private static List<SingleBuySellRecord> buysThisBlock = new ArrayList<>();
  private static List<SingleBuySellRecord> buysLastBlock = new ArrayList<>();
  private static int writeId = 0;

  private static PrintWriter mevWriter;
  private static PrintWriter userWriter;

  private static void syncMevStat(
      long startBlock,
      long recentBlock,
      long endBlock,
      Set<String> paddrs,
      Set<String> saddrs,
      String targetAddr,
      String date)
      throws FileNotFoundException {

    mevWriter = new PrintWriter("targetaddrs.txt");
    mevWriter.println("id buy_tx sell_tx user_tx user_addr block timestamp sr");
    userWriter = new PrintWriter("usertxes.txt");
    userWriter.println("id tx");

    try {

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

      Map<String, Long> swapSrMap = new HashMap<>();
      Map<String, Long> recentswapSrMap = new HashMap<>();
      Map<String, Long> pumpSrMap = new HashMap<>();
      Map<String, Long> recentpumpSrMap = new HashMap<>();

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

        buysLastBlock.clear();
        buysLastBlock.addAll(buysThisBlock);
        buysThisBlock.clear();

        byte[] value = retEntry.getValue();
        TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
        BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());

        long timestamp = transactionRetCapsule.getInstance().getBlockTimeStamp();

        Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
        String witness = get41Addr(Hex.toHexString(blockCapsule.getWitnessAddress().toByteArray()));
        for (TransactionCapsule tx : blockCapsule.getTransactions()) {
          txCallerMap.put(tx.getTransactionId().toString(), tx);
        }

        int index = -1;
        for (Protocol.TransactionInfo transactionInfo :
            transactionRetCapsule.getInstance().getTransactioninfoList()) {
          index++;
          byte[] txId = transactionInfo.getId().toByteArray();
          TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));
          String caller = get41Addr(Hex.toHexString(tx.getOwnerAddress()));
          byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
          BigDecimal fee =
              new BigDecimal(transactionInfo.getFee())
                  .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);

          String txHash = Hex.toHexString(txId);
          if (Arrays.equals(contractAddress, SWAP_ROUTER)) {

            AddrAllInfoRecord addrAllInfoRecord =
                swapAddrInfoRecordMap.getOrDefault(caller, new AddrAllInfoRecord(caller));
            AddrAllInfoRecord recentaddrAllInfoRecord =
                recentswapAddrInfoRecordMap.getOrDefault(caller, new AddrAllInfoRecord(caller));
            addrAllInfoRecord.allfee = addrAllInfoRecord.allfee.add(fee);
            if (blockNum >= recentBlock) {
              recentaddrAllInfoRecord.allfee = recentaddrAllInfoRecord.allfee.add(fee);
            }

            if (!tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.TriggerSmartContract.class)) {
              continue;
            }
            SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
                tx.getInstance()
                    .getRawData()
                    .getContract(0)
                    .getParameter()
                    .unpack(SmartContractOuterClass.TriggerSmartContract.class);
            String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());

            if (!transactionInfo.getResult().equals(SUCESS)) {
              if (!tx.getInstance()
                  .getRawData()
                  .getContract(0)
                  .getParameter()
                  .is(SmartContractOuterClass.TriggerSmartContract.class)) {
                continue;
              }
              try {
                BigDecimal tokenAmount = BigDecimal.ZERO;
                boolean isBuy = false;
                sSumTxCount++;
                if (blockNum >= recentBlock) {
                  sSumTxCountrecent++;
                }
                String token;
                if (callData.startsWith(SWAP_SELL_METHOD_1)) {
                  isBuy = false;
                  if (callData.length() < 392) {
                    token = get41Addr(callData.substring(8, 8 + 64).substring(24));
                  } else {
                    tokenAmount =
                        new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                            .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
                    String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                    String token2 = callData.substring(456).substring(24); // token2 wtrx
                    token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
                  }
                } else if (callData.startsWith(SWAP_SELL_METHOD_2)) {
                  isBuy = false;
                  tokenAmount =
                      new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                          .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
                  String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                  String token2 =
                      callData.length() >= 456
                          ? callData.substring(456).substring(24)
                          : null; // token2 wtrx
                  token =
                      (token1.equalsIgnoreCase(WTRX) && token2 != null)
                          ? get41Addr(token2)
                          : get41Addr(token1);
                } else if (callData.startsWith(SWAP_SELL_METHOD_3)) {
                  isBuy = false;
                  token = get41Addr(callData.substring(392, 392 + 64)); // token
                } else if (callData.startsWith(SWAP_METHOD)) {
                  //                String data1 = callData.substring(8, 8 + 64); // trx amount
                  String token1 = callData.substring(392, 392 + 64).substring(24); // out token
                  String token2 = callData.substring(456, 456 + 64).substring(24); // in token
                  if (token1.equalsIgnoreCase(WTRX) || token1.equalsIgnoreCase(USDT)) {
                    token = get41Addr(token2);
                    isBuy = true;
                  } else {
                    token = get41Addr(token1);
                    isBuy = false;
                  }
                } else if (callData.startsWith(SWAP_BUY_METHOD_1)) {
                  isBuy = true;
                  tokenAmount =
                      new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                          .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
                  String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                  String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
                  token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
                } else if (callData.startsWith(SWAP_BUY_METHOD_2)) {
                  isBuy = true;
                  String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                  String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
                  token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
                } else if (callData.startsWith(SWAP_BUY_METHOD_3)) {
                  isBuy = true;
                  token = get41Addr(callData.substring(392, 392 + 64)); // token
                } else {
                  // 其他方法
                  continue;
                }
                if (isBuy) {
                  sSumBuyCount++;
                  if (blockNum >= recentBlock) {
                    sSumBuyCountrecent++;
                  }
                }

                if (!saddrs.contains(caller)) {
                  continue;
                }
                AddrContinusRecord addrContinusRecord =
                    swapContinusRecordMap.getOrDefault(caller, new AddrContinusRecord(caller));
                addrContinusRecord.addRecord(
                    txHash,
                    index,
                    blockNum,
                    timestamp,
                    witness,
                    token,
                    isBuy,
                    tokenAmount,
                    BigDecimal.ZERO,
                    false,
                    fee);
                swapContinusRecordMap.put(caller, addrContinusRecord);
                if (blockNum >= recentBlock) {
                  AddrContinusRecord recentaddrContinusRecord =
                      recentswapContinusRecordMap.getOrDefault(
                          caller, new AddrContinusRecord(caller));
                  recentaddrContinusRecord.addRecord(
                      txHash,
                      index,
                      blockNum,
                      timestamp,
                      witness,
                      token,
                      isBuy,
                      tokenAmount,
                      BigDecimal.ZERO,
                      false,
                      fee);
                  recentswapContinusRecordMap.put(caller, recentaddrContinusRecord);
                }

                if (isBuy) {
                  addrAllInfoRecord.failBuy += 1;
                  if (blockNum >= recentBlock) {
                    recentaddrAllInfoRecord.failBuy += 1;
                  }
                } else {
                  addrAllInfoRecord.failSell += 1;
                  if (blockNum >= recentBlock) {
                    recentaddrAllInfoRecord.failSell += 1;
                  }
                }
                swapAddrInfoRecordMap.put(caller, addrAllInfoRecord);
                if (blockNum >= recentBlock) {
                  recentswapAddrInfoRecordMap.put(caller, recentaddrAllInfoRecord);
                }

                continue;
              } catch (Exception e) {
                continue;
              }
            }

            // 成功交易
            // Swap tx
            sSumTxCount++;
            if (blockNum >= recentBlock) {
              sSumTxCountrecent++;
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

              if (!saddrs.contains(caller)) {
                if (isBuy) {
                  buysThisBlock.add(
                      new SingleBuySellRecord(
                          txHash,
                          caller,
                          index,
                          token,
                          isBuy,
                          tokenAmount,
                          BigDecimal.ZERO,
                          blockNum,
                          timestamp,
                          witness,
                          false,
                          true,
                          fee,
                          false));
                }
                continue;
              }
              // 这里只记录
              AddrContinusRecord addrContinusRecord =
                  swapContinusRecordMap.getOrDefault(caller, new AddrContinusRecord(caller));
              addrContinusRecord.addRecord(
                  txHash,
                  index,
                  blockNum,
                  timestamp,
                  witness,
                  token,
                  isBuy,
                  tokenAmount,
                  trxAmount,
                  true,
                  fee);
              swapContinusRecordMap.put(caller, addrContinusRecord);
              if (blockNum >= recentBlock) {
                AddrContinusRecord recentaddrContinusRecord =
                    recentswapContinusRecordMap.getOrDefault(
                        caller, new AddrContinusRecord(caller));
                recentaddrContinusRecord.addRecord(
                    txHash,
                    index,
                    blockNum,
                    timestamp,
                    witness,
                    token,
                    isBuy,
                    tokenAmount,
                    trxAmount,
                    true,
                    fee);
                recentswapContinusRecordMap.put(caller, recentaddrContinusRecord);
              }
              if (isBuy) {
                addrAllInfoRecord.successBuy += 1;
                if (blockNum >= recentBlock) {
                  recentaddrAllInfoRecord.successBuy += 1;
                }
              } else {
                addrAllInfoRecord.successSell += 1;
                if (blockNum >= recentBlock) {
                  recentaddrAllInfoRecord.successSell += 1;
                }
              }
              swapAddrInfoRecordMap.put(caller, addrAllInfoRecord);
              if (blockNum >= recentBlock) {
                recentswapAddrInfoRecordMap.put(caller, recentaddrAllInfoRecord);
              }
              // 找到目标log就跳出
              break;
            }
          } else if (Arrays.equals(contractAddress, SUNSWAP_ROUTER)) {
            if (!tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.TriggerSmartContract.class)) {
              continue;
            }
            SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
                tx.getInstance()
                    .getRawData()
                    .getContract(0)
                    .getParameter()
                    .unpack(SmartContractOuterClass.TriggerSmartContract.class);
            if (transactionInfo.getResult().equals(SUCESS)) {
              try {
                String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());
                BigDecimal tokenAmount = BigDecimal.ZERO;
                boolean isBuy = false;
                String token;
                if (callData.startsWith(SWAP_METHOD)) {
                  //                String data1 = callData.substring(8, 8 + 64); //trx amount
                  String token1 = callData.substring(392, 392 + 64).substring(24); // out token
                  String token2 = callData.substring(456, 456 + 64).substring(24); // in token
                  String token3 =
                      callData.length() > 520
                          ? callData.substring(520, 520 + 64).substring(24)
                          : null;
                  if (token3 != null
                      && !token3.equalsIgnoreCase(WTRX)
                      && !token3.equalsIgnoreCase(USDT)) {
                    token = get41Addr(token3);
                    isBuy = true;
                  } else if (token1.equalsIgnoreCase(WTRX) || token1.equalsIgnoreCase(USDT)) {
                    token = get41Addr(token2);
                    isBuy = true;
                  } else {
                    token = get41Addr(token1);
                    isBuy = false;
                  }
                } else if (callData.startsWith(SWAP_BUY_METHOD_1)) {
                  isBuy = true;
                  tokenAmount =
                      new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                          .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
                  String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                  String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
                  token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
                } else if (callData.startsWith(SWAP_BUY_METHOD_2)) {
                  isBuy = true;
                  String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                  String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
                  token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
                } else if (callData.startsWith(SWAP_BUY_METHOD_3)) {
                  isBuy = true;
                  token = get41Addr(callData.substring(392, 392 + 64)); // token
                } else {
                  // 其他方法
                  continue;
                }
                if (isBuy) {
                  buysThisBlock.add(
                      new SingleBuySellRecord(
                          txHash,
                          caller,
                          index,
                          token,
                          isBuy,
                          tokenAmount,
                          BigDecimal.ZERO,
                          blockNum,
                          timestamp,
                          witness,
                          false,
                          true,
                          fee,
                          false));
                }

              } catch (Exception e) {
              }
            }

          } else if (Arrays.equals(contractAddress, SUNPUMP_LAUNCH)) {

            if (!tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.TriggerSmartContract.class)) {
              continue;
            }

            AddrAllInfoRecord addrAllInfoRecord =
                pumpAddrInfoRecordMap.getOrDefault(caller, new AddrAllInfoRecord(caller));
            AddrAllInfoRecord recentaddrAllInfoRecord =
                recentpumpAddrInfoRecordMap.getOrDefault(caller, new AddrAllInfoRecord(caller));
            addrAllInfoRecord.allfee = addrAllInfoRecord.allfee.add(fee);
            if (blockNum >= recentBlock) {
              recentaddrAllInfoRecord.allfee = recentaddrAllInfoRecord.allfee.add(fee);
            }
            SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
                tx.getInstance()
                    .getRawData()
                    .getContract(0)
                    .getParameter()
                    .unpack(SmartContractOuterClass.TriggerSmartContract.class);
            // 只遍历成功交易
            if (!transactionInfo.getResult().equals(SUCESS)) {
              pSumTxCount += 1;
              if (blockNum >= recentBlock) {
                pSumTxCountrecent += 1;
              }
              try {
                String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());
                BigDecimal tokenAmount = BigDecimal.ZERO;
                boolean isBuy = false;
                String token;
                if (callData.startsWith(PUMP_SELL_METHOD)) {
                  isBuy = false;
                  token = get41Addr(callData.substring(8, 8 + 64).substring(24)); // token
                  tokenAmount =
                      new BigDecimal(new BigInteger(callData.substring(72, 72 + 64), 16))
                          .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // amount
                } else if (callData.startsWith(PUMP_BUY_METHOD_1)) {
                  isBuy = true;
                  token = get41Addr(callData.substring(8, 8 + 64).substring(24)); // token
                } else {
                  // 其他方法
                  continue;
                }
                if (isBuy) {
                  pSumBuyCount += 1;
                  if (blockNum >= recentBlock) {
                    pSumBuyCountrecent += 1;
                  }
                }
                if (!paddrs.contains(caller)) {
                  continue;
                }
                AddrContinusRecord addrContinusRecord =
                    pumpContinusRecordMap.getOrDefault(caller, new AddrContinusRecord(caller));
                addrContinusRecord.addRecord(
                    txHash,
                    index,
                    blockNum,
                    timestamp,
                    witness,
                    token,
                    isBuy,
                    tokenAmount,
                    BigDecimal.ZERO,
                    false,
                    fee);
                pumpContinusRecordMap.put(caller, addrContinusRecord);
                if (blockNum >= recentBlock) {
                  AddrContinusRecord recentaddrContinusRecord =
                      recentpumpContinusRecordMap.getOrDefault(
                          caller, new AddrContinusRecord(caller));
                  recentaddrContinusRecord.addRecord(
                      txHash,
                      index,
                      blockNum,
                      timestamp,
                      witness,
                      token,
                      isBuy,
                      tokenAmount,
                      BigDecimal.ZERO,
                      false,
                      fee);
                  recentpumpContinusRecordMap.put(caller, recentaddrContinusRecord);
                }
                if (isBuy) {
                  addrAllInfoRecord.failBuy += 1;
                  if (blockNum >= recentBlock) {
                    recentaddrAllInfoRecord.failBuy += 1;
                  }
                } else {
                  addrAllInfoRecord.failSell += 1;
                  if (blockNum >= recentBlock) {
                    recentaddrAllInfoRecord.failSell += 1;
                  }
                }
                pumpAddrInfoRecordMap.put(caller, addrAllInfoRecord);
                if (blockNum >= recentBlock) {
                  recentpumpAddrInfoRecordMap.put(caller, recentaddrAllInfoRecord);
                }
                continue;

              } catch (Exception e) {
                continue;
              }
            }
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
                trxAmount =
                    new BigDecimal(triggerSmartContract.getCallValue())
                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                //                BigDecimal trxAmount1 =
                //                    new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
                //                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                //                BigDecimal feeAmount1 =
                //                    new BigDecimal(new BigInteger(dataStr.substring(64, 128), 16))
                //                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                //                BigDecimal trxAmount2 = BigDecimal.ZERO;
                //                for (Protocol.TransactionInfo.Log log2 :
                // transactionInfo.getLogList()) {
                //                  if (Arrays.equals(log2.getTopics(0).toByteArray(),
                // TRX_RECEIVED)) {
                //                    trxAmount2 =
                //                        trxAmount2.add(
                //                            new BigDecimal(
                //                                    new BigInteger(
                //
                // Hex.toHexString(log2.getData().toByteArray()), 16))
                //                                .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN));
                //                  }
                //                }
                //                    trxAmount1.compareTo(trxAmount2) > 0 ?
                // trxAmount1.add(feeAmount1) : trxAmount2;
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
              addrContinusRecord.addRecord(
                  txHash,
                  index,
                  blockNum,
                  timestamp,
                  witness,
                  token,
                  isBuy,
                  tokenAmount,
                  trxAmount,
                  true,
                  fee);
              pumpContinusRecordMap.put(caller, addrContinusRecord);
              if (blockNum >= recentBlock) {
                AddrContinusRecord recentaddrContinusRecord =
                    recentpumpContinusRecordMap.getOrDefault(
                        caller, new AddrContinusRecord(caller));
                recentaddrContinusRecord.addRecord(
                    txHash,
                    index,
                    blockNum,
                    timestamp,
                    witness,
                    token,
                    isBuy,
                    tokenAmount,
                    trxAmount,
                    true,
                    fee);
                recentpumpContinusRecordMap.put(caller, recentaddrContinusRecord);
              }

              if (isBuy) {
                addrAllInfoRecord.successBuy += 1;
                if (blockNum >= recentBlock) {
                  recentaddrAllInfoRecord.successBuy += 1;
                }
              } else {
                addrAllInfoRecord.successSell += 1;
                if (blockNum >= recentBlock) {
                  recentaddrAllInfoRecord.successSell += 1;
                }
              }
              pumpAddrInfoRecordMap.put(caller, addrAllInfoRecord);
              if (blockNum >= recentBlock) {
                recentpumpAddrInfoRecordMap.put(caller, recentaddrAllInfoRecord);
              }
              // 找到目标log就跳出
              break;
            }
          }
        }
        // 当前块交易结束, 处理这个块的买卖，并更新记录
        boolean swapSuccess =
            proceeToBlock(
                swapContinusRecordMap,
                swapAddrInfoRecordMap,
                blockNum,
                witness,
                swapToRecordSrAddrs);
        if (swapSuccess) {
          swapSrMap.put(witness, swapSrMap.getOrDefault(witness, 0L) + 1);
        }
        boolean pumpSuccess =
            proceeToBlock(
                pumpContinusRecordMap,
                pumpAddrInfoRecordMap,
                blockNum,
                witness,
                pumpToRecordSrAddrs);
        if (pumpSuccess) {
          pumpSrMap.put(witness, pumpSrMap.getOrDefault(witness, 0L) + 1);
        }
        if (blockNum >= recentBlock) {
          boolean recentswapSuccess =
              proceeToBlock(
                  recentswapContinusRecordMap,
                  recentswapAddrInfoRecordMap,
                  blockNum,
                  witness,
                  new HashSet<>());
          if (recentswapSuccess) {
            recentswapSrMap.put(witness, recentswapSrMap.getOrDefault(witness, 0L) + 1);
          }
          boolean recentpumpSuccess =
              proceeToBlock(
                  recentpumpContinusRecordMap,
                  recentpumpAddrInfoRecordMap,
                  blockNum,
                  witness,
                  new HashSet<>());
          if (recentpumpSuccess) {
            recentpumpSrMap.put(witness, recentpumpSrMap.getOrDefault(witness, 0L) + 1);
          }
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
      //      if (true) {
      // 跑地址
      //        PrintWriter pwriter = new PrintWriter("finalresult.txt");
      //        pwriter.println("SWAP");
      //        swapAddrInfoRecordMap.forEach(
      //            (k, v) -> {
      //              if (v.getAllAttack() > 0) {
      //                pwriter.println(StringUtil.encode58Check(Hex.decode(k)));
      //              }
      //            });
      //        pwriter.println("PUMP");
      //        pumpAddrInfoRecordMap.forEach(
      //            (k, v) -> {
      //              if (v.getAllAttack() > 0) {
      //                pwriter.println(StringUtil.encode58Check(Hex.decode(k)));
      //              }
      //            });
      //        pwriter.close();
      //        logger.info("Tmp end!");
      //        return;
      //      }

      //      if (true) {
      //
      AddrAllInfoRecord record = swapAddrInfoRecordMap.get(targetAddr);
      if (record == null) {
        record = new AddrAllInfoRecord(targetAddr);
      }
      String msg =
          date
              + " "
              + StringUtil.encode58Check(Hex.decode(targetAddr))
              + " "
              + record.getSuccessCount()
              + " "
              + record.getAllProfit()
              + " "
              + record.getLackCount()
              + " "
              + record.getAllLack()
              + " "
              + record.getAllAttack()
              + " "
              + record.getAllAttackTarget()
              + " "
              + record.getTrxOutAmount()
              + " "
              + record.getMzSucCount()
              + " "
              + record.getMzLackCount()
              + " "
              + record.getFee()
              + " "
              + record.getAllfee()
              + " "
              + record.getSuccessBuy()
              + " "
              + record.getSuccessSell()
              + " "
              + record.getFailBuy()
              + " "
              + record.getFailSell();
      logger.info("End syncing from {} to {}, \n {}", startBlock, endBlock, msg);
      System.out.println(msg);
      //        return;
      //      }

      //      // 输出结果
      //      PrintWriter pwriter = new PrintWriter("finalresult.txt");
      //      pwriter.println("SWAP");
      //      swapAddrInfoRecordMap.forEach(
      //          (k, v) ->
      //              pwriter.println(
      //                  StringUtil.encode58Check(Hex.decode(k))
      //                      + " "
      //                      + v.getSuccessCount()
      //                      + " "
      //                      + v.getAllProfit()
      //                      + " "
      //                      + v.getLackCount()
      //                      + " "
      //                      + v.getAllLack()
      //                      + " "
      //                      + v.getAllAttack()
      //                      + " "
      //                      + v.getAllAttackTarget()
      //                      + " "
      //                      + v.getTrxOutAmount()
      //                      + " "
      //                      + v.getMzSucCount()
      //                      + " "
      //                      + v.getMzLackCount()
      //                      + " "
      //                      + v.getFee()
      //                      + " "
      //                      + v.getAllfee()
      //                      + " "
      //                      + v.getSuccessBuy()
      //                      + " "
      //                      + v.getSuccessSell()
      //                      + " "
      //                      + v.getFailBuy()
      //                      + " "
      //                      + v.getFailSell()));
      //      pwriter.println("RECENTSWAP");
      //      recentswapAddrInfoRecordMap.forEach(
      //          (k, v) ->
      //              pwriter.println(
      //                  StringUtil.encode58Check(Hex.decode(k))
      //                      + " "
      //                      + v.getSuccessCount()
      //                      + " "
      //                      + v.getAllProfit()
      //                      + " "
      //                      + v.getLackCount()
      //                      + " "
      //                      + v.getAllLack()
      //                      + " "
      //                      + v.getAllAttack()
      //                      + " "
      //                      + v.getAllAttackTarget()
      //                      + " "
      //                      + v.getTrxOutAmount()
      //                      + " "
      //                      + v.getMzSucCount()
      //                      + " "
      //                      + v.getMzLackCount()
      //                      + " "
      //                      + v.getFee()
      //                      + " "
      //                      + v.getAllfee()
      //                      + " "
      //                      + v.getSuccessBuy()
      //                      + " "
      //                      + v.getSuccessSell()
      //                      + " "
      //                      + v.getFailBuy()
      //                      + " "
      //                      + v.getFailSell()));
      //      pwriter.println("PUMP");
      //      pumpAddrInfoRecordMap.forEach(
      //          (k, v) ->
      //              pwriter.println(
      //                  StringUtil.encode58Check(Hex.decode(k))
      //                      + " "
      //                      + v.getSuccessCount()
      //                      + " "
      //                      + v.getAllProfit()
      //                      + " "
      //                      + v.getLackCount()
      //                      + " "
      //                      + v.getAllLack()
      //                      + " "
      //                      + v.getAllAttack()
      //                      + " "
      //                      + v.getAllAttackTarget()
      //                      + " "
      //                      + v.getTrxOutAmount()
      //                      + " "
      //                      + v.getMzSucCount()
      //                      + " "
      //                      + v.getMzLackCount()
      //                      + " "
      //                      + v.getFee()
      //                      + " "
      //                      + v.getAllfee()
      //                      + " "
      //                      + v.getSuccessBuy()
      //                      + " "
      //                      + v.getSuccessSell()
      //                      + " "
      //                      + v.getFailBuy()
      //                      + " "
      //                      + v.getFailSell()));
      //      pwriter.println("RECENTPUMP");
      //      recentpumpAddrInfoRecordMap.forEach(
      //          (k, v) ->
      //              pwriter.println(
      //                  StringUtil.encode58Check(Hex.decode(k))
      //                      + " "
      //                      + v.getSuccessCount()
      //                      + " "
      //                      + v.getAllProfit()
      //                      + " "
      //                      + v.getLackCount()
      //                      + " "
      //                      + v.getAllLack()
      //                      + " "
      //                      + v.getAllAttack()
      //                      + " "
      //                      + v.getAllAttackTarget()
      //                      + " "
      //                      + v.getTrxOutAmount()
      //                      + " "
      //                      + v.getMzSucCount()
      //                      + " "
      //                      + v.getMzLackCount()
      //                      + " "
      //                      + v.getFee()
      //                      + " "
      //                      + v.getAllfee()
      //                      + " "
      //                      + v.getSuccessBuy()
      //                      + " "
      //                      + v.getSuccessSell()
      //                      + " "
      //                      + v.getFailBuy()
      //                      + " "
      //                      + v.getFailSell()));
      //
      //      pwriter.println("TOKEN_REMAINING");
      //      pwriter.println("SWAPTOKEN");
      //      swapAddrInfoRecordMap.forEach(
      //          (k, v) -> {
      //            pwriter.println("Addr " + StringUtil.encode58Check(Hex.decode(k)));
      //            v.tokenRecords.forEach(
      //                (k1, v1) -> {
      //                  if (v1.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
      //                    pwriter.println(
      //                        "Token "
      //                            + StringUtil.encode58Check(Hex.decode(k1))
      //                            + " "
      //                            + v1.remainingTokenAmount
      //                            + " "
      //                            + v1.trxOutAmount);
      //                  }
      //                });
      //          });
      //      pwriter.println("RECENTSWAPTOKEN");
      //      recentswapAddrInfoRecordMap.forEach(
      //          (k, v) -> {
      //            pwriter.println("Addr " + StringUtil.encode58Check(Hex.decode(k)));
      //            v.tokenRecords.forEach(
      //                (k1, v1) -> {
      //                  if (v1.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
      //                    pwriter.println(
      //                        "Token "
      //                            + StringUtil.encode58Check(Hex.decode(k1))
      //                            + " "
      //                            + v1.remainingTokenAmount
      //                            + " "
      //                            + v1.trxOutAmount);
      //                  }
      //                });
      //          });
      //      pwriter.println("PUMPTOKEN");
      //      pumpAddrInfoRecordMap.forEach(
      //          (k, v) -> {
      //            pwriter.println("Addr " + StringUtil.encode58Check(Hex.decode(k)));
      //            v.tokenRecords.forEach(
      //                (k1, v1) -> {
      //                  if (v1.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
      //                    pwriter.println(
      //                        "Token "
      //                            + StringUtil.encode58Check(Hex.decode(k1))
      //                            + " "
      //                            + v1.remainingTokenAmount
      //                            + " "
      //                            + v1.trxOutAmount);
      //                  }
      //                });
      //          });
      //      pwriter.println("RECENTPUMPTOKEN");
      //      recentpumpAddrInfoRecordMap.forEach(
      //          (k, v) -> {
      //            pwriter.println("Addr " + StringUtil.encode58Check(Hex.decode(k)));
      //            v.tokenRecords.forEach(
      //                (k1, v1) -> {
      //                  if (v1.getRemainingTokenAmount().compareTo(BigDecimal.ONE) > 0) {
      //                    pwriter.println(
      //                        "Token "
      //                            + StringUtil.encode58Check(Hex.decode(k1))
      //                            + " "
      //                            + v1.remainingTokenAmount
      //                            + " "
      //                            + v1.trxOutAmount);
      //                  }
      //                });
      //          });
      //
      //      pwriter.close();
      //
      //      PrintWriter srwriter = new PrintWriter("finalsr.txt");
      //      srwriter.println("SWAP");
      //      swapSrMap.forEach(
      //          (k, v) -> srwriter.println(StringUtil.encode58Check(Hex.decode(k)) + " " + v));
      //      srwriter.println("RECENTSWAP");
      //      recentswapSrMap.forEach(
      //          (k, v) -> srwriter.println(StringUtil.encode58Check(Hex.decode(k)) + " " + v));
      //      srwriter.println("PUMP");
      //      pumpSrMap.forEach(
      //          (k, v) -> srwriter.println(StringUtil.encode58Check(Hex.decode(k)) + " " + v));
      //      srwriter.println("RECENTPUMP");
      //      recentpumpSrMap.forEach(
      //          (k, v) -> srwriter.println(StringUtil.encode58Check(Hex.decode(k)) + " " + v));
      //
      //      srwriter.println("SWAPTOP");
      //      swapToRecordSrAddrs.forEach(
      //          addr -> {
      //            srwriter.println(StringUtil.encode58Check(Hex.decode(addr)));
      //            AddrAllInfoRecord record =
      //                swapAddrInfoRecordMap.getOrDefault(addr, new AddrAllInfoRecord(addr));
      //            record.successSrBlock.forEach(
      //                (k, v) -> srwriter.println(StringUtil.encode58Check(Hex.decode(k)) + " " +
      // v));
      //          });
      //
      //      srwriter.println("PUMPTOP");
      //      pumpToRecordSrAddrs.forEach(
      //          addr -> {
      //            srwriter.println(StringUtil.encode58Check(Hex.decode(addr)));
      //            AddrAllInfoRecord record =
      //                pumpAddrInfoRecordMap.getOrDefault(addr, new AddrAllInfoRecord(addr));
      //            record.successSrBlock.forEach(
      //                (k, v) -> srwriter.println(StringUtil.encode58Check(Hex.decode(k)) + " " +
      // v));
      //          });
      //
      //      srwriter.close();
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

    mevWriter.close();
    userWriter.close();

    logger.info("END");
  }

  private static boolean proceeToBlock(
      Map<String, AddrContinusRecord> continusRecordMap,
      Map<String, AddrAllInfoRecord> addrAllInfoRecordMap,
      long blockNum,
      String witness,
      Set<String> toRecordWitness) {
    boolean blockSuccess = false;
    for (Map.Entry<String, AddrContinusRecord> entry : continusRecordMap.entrySet()) {
      boolean addrBlockSuccess = false;
      // 每个人
      String addr = entry.getKey();
      boolean toRecordSr = toRecordWitness.contains(addr);
      AddrAllInfoRecord addrAllInfoRecord =
          addrAllInfoRecordMap.getOrDefault(addr, new AddrAllInfoRecord(addr));
      BigDecimal feeToAdd = BigDecimal.ZERO;

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
        long buyFeeAddedLastBlock = 0;
        long buyFeeAddedThisBlock = 0;
        long sellFeeAddedLastBlock = 0;
        long sellFeeAddedThisBlock = 0;
        boolean tokenBlockSuccess =
            buySellsLastBlocks.blockSuccess || buySellsThisBlocks.blockSuccess;

        // 第一遍，match上的
        for (int i = 0; i < buySellsThisBlocks.records.size(); i++) {
          // caller 每个 token 买卖
          SingleBuySellRecord buySell = buySellsThisBlocks.records.get(i);
          if (buySell.isSuccess()) {
            if (buySell.isBuy()) {
              buySellsThisBlocks.addBuyCount();
              addrAllInfoRecord.addBuy();
            } else {
              buySellsThisBlocks.addSellCount();
              // 卖，最近两块匹配
              boolean curTokenBlockSuccess =
                  matchBuySell(
                      buySell,
                      buySellsLastBlocks.records,
                      addrAllInfoRecord,
                      token,
                      buySellsLastBlocks.records.size(),
                      true,
                      tokenBlockSuccess);
              tokenBlockSuccess = tokenBlockSuccess || curTokenBlockSuccess;
              if (!buySell.matched) {
                curTokenBlockSuccess =
                    matchBuySell(
                        buySell,
                        buySellsThisBlocks.records,
                        addrAllInfoRecord,
                        token,
                        buySellsThisBlocks.records.size(),
                        false,
                        tokenBlockSuccess);
                tokenBlockSuccess = tokenBlockSuccess || curTokenBlockSuccess;
              }
            }
          } else {
            // 失败的，先记次数
            if (buySell.isBuy()) {
              buySellsThisBlocks.addBuyCount();
            } else {
              buySellsThisBlocks.addSellCount();
            }
          }
        }
        buySellsThisBlocks.remainingSellAvailable =
            buySellsLastBlocks.buyCount > 0 || buySellsThisBlocks.buyCount > 0;
        buySellsThisBlocks.remainingBuyAvailable =
            buySellsLastBlocks.sellCount > 0 || buySellsThisBlocks.sellCount > 0;

        for (int i = 0; i < buySellsThisBlocks.records.size(); i++) {
          SingleBuySellRecord buySell = buySellsThisBlocks.records.get(i);
          if (buySell.success && buySell.matched) {
            feeToAdd = feeToAdd.add(buySell.fee);
            if (buySell.isBuy) {
              buyFeeAddedThisBlock++;
            } else {
              sellFeeAddedThisBlock++;
            }
          }
        }
        for (int i = 0; i < buySellsLastBlocks.records.size(); i++) {
          SingleBuySellRecord buySell = buySellsLastBlocks.records.get(i);
          if (buySell.success && buySell.matched) {
            feeToAdd = feeToAdd.add(buySell.fee);
            if (buySell.isBuy) {
              buyFeeAddedLastBlock++;
            } else {
              sellFeeAddedLastBlock++;
            }
          }
        }

        // 清空matched 和 失败的，因为失败的已经记录次数了
        buySellsThisBlocks.removeMatched();
        buySellsLastBlocks.removeMatched();

        // 第二遍找没匹配上的中，取较小值比较
        // this block
        // last block
        List<SingleBuySellRecord> sellsLastBlock =
            buySellsLastBlocks.records.stream()
                .filter(SingleBuySellRecord::isSuccess)
                .filter(sell -> !sell.isBuy)
                .collect(Collectors.toList());
        List<SingleBuySellRecord> buyLastBlock =
            buySellsLastBlocks.records.stream()
                .filter(SingleBuySellRecord::isSuccess)
                .filter(buy -> buy.isBuy)
                .collect(Collectors.toList());
        TokenAllInfoRecord tokenAllInfoRecord = addrAllInfoRecord.getTokenAllInfoRecord(token);

        Iterator<SingleBuySellRecord> sellRecordIterator = sellsLastBlock.iterator();
        while (sellRecordIterator.hasNext()) {
          SingleBuySellRecord sell = sellRecordIterator.next();

          Iterator<SingleBuySellRecord> buyRecordIterator = buyLastBlock.iterator();
          while (buyRecordIterator.hasNext()) {
            SingleBuySellRecord buy = buyRecordIterator.next();
            BigDecimal actualTokenAmount =
                sell.getTokenAmount().compareTo(buy.getTokenAmount()) > 0
                    ? buy.getTokenAmount()
                    : sell.getTokenAmount();
            BigDecimal actualGetTrxAmount = sell.getActualTrxAmount(actualTokenAmount);
            BigDecimal actualOutTrxAmount = buy.getActualTrxAmount(actualTokenAmount);
            BigDecimal profit = actualGetTrxAmount.subtract(actualOutTrxAmount);
            if (profit.compareTo(BigDecimal.ZERO) > 0) {
              addrBlockSuccess = true;
            }
            addrAllInfoRecord.addTokenRecord(token, profit, true);

            buy.subTokenAmount(actualTokenAmount);
            sell.subTokenAmount(actualTokenAmount);

            if (!buy.feeAdded) {
              feeToAdd = feeToAdd.add(buy.fee);
              buy.feeAdded = true;
              buyFeeAddedLastBlock++;
            }
            if (!sell.feeAdded) {
              feeToAdd = feeToAdd.add(sell.fee);
              sell.feeAdded = true;
              sellFeeAddedLastBlock++;
            }

            if (buy.isMatched()) {
              buyRecordIterator.remove();
            }

            if (sell.isMatched()) {
              sellRecordIterator.remove();
              break;
            }
          }
        }

        // 上个块卖可以平账，最近2块有卖有卖
        //        if (!sellsLastBlock.isEmpty() && buySellsLastBlocks.remainingSellAvailable) {
        if (!sellsLastBlock.isEmpty()) {
          BigDecimal tokenAmount = BigDecimal.ZERO;
          BigDecimal trxAmount = BigDecimal.ZERO;
          for (SingleBuySellRecord sell : sellsLastBlock) {
            tokenAmount = tokenAmount.add(sell.getTokenAmount());
            trxAmount = trxAmount.add(sell.getTrxAmount());
            //            if (!sell.feeAdded) {
            //              feeToAdd = feeToAdd.add(sell.fee);
            //              sell.feeAdded = true;
            //              sellFeeAddedLastBlock++;
            //            }
          }
          // 平账
          tokenAllInfoRecord.removeRemaining(tokenAmount, trxAmount);
        }

        if (buySellsThisBlocks.hasSuccessSell() && !buyLastBlock.isEmpty()) {

          Iterator<SingleBuySellRecord> sellRecordThisBlockIterator =
              buySellsThisBlocks.records.iterator();
          while (sellRecordThisBlockIterator.hasNext()) {
            SingleBuySellRecord sell = sellRecordThisBlockIterator.next();
            if (!sell.isSuccess()) {
              continue;
            }

            Iterator<SingleBuySellRecord> buyRecordIterator = buyLastBlock.iterator();
            while (buyRecordIterator.hasNext()) {
              SingleBuySellRecord buy = buyRecordIterator.next();
              BigDecimal actualTokenAmount =
                  sell.getTokenAmount().compareTo(buy.getTokenAmount()) > 0
                      ? buy.getTokenAmount()
                      : sell.getTokenAmount();
              BigDecimal actualGetTrxAmount = sell.getActualTrxAmount(actualTokenAmount);
              BigDecimal actualOutTrxAmount = buy.getActualTrxAmount(actualTokenAmount);
              BigDecimal profit = actualGetTrxAmount.subtract(actualOutTrxAmount);
              if (profit.compareTo(BigDecimal.ZERO) > 0) {
                addrBlockSuccess = true;
              }
              addrAllInfoRecord.addTokenRecord(token, profit, true);

              buy.subTokenAmount(actualTokenAmount);
              sell.subTokenAmount(actualTokenAmount);
              if (!buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (!sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedThisBlock++;
              }

              if (buy.isMatched()) {
                buyRecordIterator.remove();
              }

              if (sell.isMatched()) {
                sellRecordThisBlockIterator.remove();
                break;
              }
            }
          }
        }

        if (!buyLastBlock.isEmpty()
            && (buySellsLastBlocks.remainingBuyAvailable
                || buySellsThisBlocks.remainingBuyAvailable)) {
          // 全部匹配完还有剩余，记账
          BigDecimal tokenAmount = BigDecimal.ZERO;
          BigDecimal trxAmount = BigDecimal.ZERO;
          for (SingleBuySellRecord buy : buyLastBlock) {
            tokenAmount = tokenAmount.add(buy.getTokenAmount());
            trxAmount = trxAmount.add(buy.getTrxAmount());
            //            if (!buy.feeAdded) {
            //              feeToAdd = feeToAdd.add(buy.fee);
            //              buy.feeAdded = true;
            //              buyFeeAddedLastBlock++;
            //            }
          }
          // 记账
          tokenAllInfoRecord.addRemaining(tokenAmount, trxAmount);
        }
        // 本块记录移到上一个块
        //        buySellsThisBlocks.updateRecords(buyThisBlock, sellsThisBlock);
        addrAllInfoRecord.updateTokenAllInfoRecord(token, tokenAllInfoRecord);
        addrAllInfoRecordMap.put(addr, addrAllInfoRecord);

        long attackCountLastBlock =
            Math.min(
                buySellsLastBlocks.availableAttackBuyCount(),
                buySellsLastBlocks.availableAttackSellCount());

        long attackCountToRecord = attackCountLastBlock;
        // fee
        if (buyFeeAddedLastBlock < attackCountLastBlock) {
          for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
            if (buy.isBuy && !buy.feeAdded) {
              feeToAdd = feeToAdd.add(buy.fee);
              buy.feeAdded = true;
              buyFeeAddedLastBlock++;
            }
            if (buyFeeAddedLastBlock == attackCountLastBlock) {
              break;
            }
          }
        }
        if (sellFeeAddedLastBlock < attackCountLastBlock) {
          for (SingleBuySellRecord sell : buySellsLastBlocks.records) {
            if (!sell.isBuy && !sell.feeAdded) {
              feeToAdd = feeToAdd.add(sell.fee);
              sell.feeAdded = true;
              sellFeeAddedLastBlock++;
            }
            if (sellFeeAddedLastBlock == attackCountLastBlock) {
              break;
            }
          }
        }

        long remainingBuyCount =
            buySellsLastBlocks.availableAttackBuyCount() - attackCountLastBlock;
        long attackCountTwoBlock =
            Math.min(remainingBuyCount, buySellsThisBlocks.availableAttackSellCount());
        if (attackCountTwoBlock > 0) {
          attackCountToRecord += attackCountTwoBlock;
          buySellsThisBlocks.recordSellCount += attackCountTwoBlock;
          // fee
          if (buyFeeAddedLastBlock < attackCountTwoBlock + attackCountLastBlock) {
            for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
              if (buy.isBuy && !buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (buyFeeAddedLastBlock == attackCountTwoBlock + attackCountLastBlock) {
                break;
              }
            }
          }
          if (sellFeeAddedThisBlock < attackCountTwoBlock) {
            for (SingleBuySellRecord sell : buySellsThisBlocks.records) {
              if (!sell.isBuy && !sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedThisBlock++;
              }
              if (sellFeeAddedThisBlock == attackCountTwoBlock) {
                break;
              }
            }
          }
        }

        long attackCountThisBlock =
            Math.min(
                buySellsThisBlocks.availableAttackBuyCount(),
                buySellsThisBlocks.availableAttackSellCount());
        attackCountToRecord += attackCountThisBlock;
        buySellsThisBlocks.recordBuyCount += attackCountThisBlock;
        buySellsThisBlocks.recordSellCount += attackCountThisBlock;
        // fee
        if (buyFeeAddedThisBlock < attackCountTwoBlock + attackCountThisBlock) {
          for (SingleBuySellRecord buy : buySellsThisBlocks.records) {
            if (buy.isBuy && !buy.feeAdded) {
              feeToAdd = feeToAdd.add(buy.fee);
              buy.feeAdded = true;
              buyFeeAddedThisBlock++;
            }
            if (buyFeeAddedThisBlock == attackCountTwoBlock + attackCountThisBlock) {
              break;
            }
          }
        }
        if (sellFeeAddedThisBlock < attackCountTwoBlock + attackCountThisBlock) {
          for (SingleBuySellRecord sell : buySellsThisBlocks.records) {
            if (!sell.isBuy && !sell.feeAdded) {
              feeToAdd = feeToAdd.add(sell.fee);
              sell.feeAdded = true;
              sellFeeAddedThisBlock++;
            }
            if (sellFeeAddedThisBlock == attackCountTwoBlock + attackCountThisBlock) {
              break;
            }
          }
        }

        tokenAllInfoRecord.addAttack(attackCountToRecord);
        if (buySellsLastBlocks.attackTargetCount == 0
            && ((buySellsLastBlocks.buyCount + buySellsThisBlocks.buyCount > 0)
                && (buySellsLastBlocks.sellCount + buySellsThisBlocks.sellCount > 0))) {
          tokenAllInfoRecord.addAttackTarget(1);
          buySellsLastBlocks.attackTargetCount++;
          buySellsThisBlocks.attackTargetCount++;
        }

        if (!buySellsLastBlocks.blockSuccess
            && !buySellsThisBlocks.blockSuccess
            && tokenBlockSuccess) {
          tokenAllInfoRecord.addSuccessCount();
          buySellsLastBlocks.blockSuccess = true;
          buySellsThisBlocks.blockSuccess = true;
        }
        if (tokenBlockSuccess) {
          buySellsLastBlocks.blockSuccess = true;
          buySellsThisBlocks.blockSuccess = true;
          addrBlockSuccess = tokenBlockSuccess;
          blockSuccess = tokenBlockSuccess;
        }

        if ((buySellsLastBlocks.buyCount + buySellsThisBlocks.buyCount > 0)
            && (buySellsLastBlocks.sellCount + buySellsThisBlocks.sellCount > 0)) {
          for (SingleBuySellRecord record : buySellsLastBlocks.records) {
            if (!record.feeAdded) {
              feeToAdd = feeToAdd.add(record.fee);
              record.feeAdded = true;
            }
          }
        }

        // update
        thisBlockRecords.put(token, buySellsThisBlocks);
        addrTwoBlockRecord.updateRecordsByBlockNum(blockNum, thisBlockRecords);
        continusRecordMap.put(addr, addrTwoBlockRecord);

        lastBlockRecords.remove(token);
      }

      // 处理完本块交易，再看上一个块未匹配上的token
      if (!lastBlockRecords.isEmpty()) {
        long buyFeeAddedLastBlock = 0;
        long sellFeeAddedLastBlock = 0;
        for (Map.Entry<String, ContinusBlockRecord> tokenEntry : lastBlockRecords.entrySet()) {
          ContinusBlockRecord buySellsLastBlocks = tokenEntry.getValue();
          List<SingleBuySellRecord> sellsLastBlock =
              buySellsLastBlocks.records.stream()
                  .filter(SingleBuySellRecord::isSuccess)
                  .filter(sell -> !sell.isBuy)
                  .collect(Collectors.toList());
          List<SingleBuySellRecord> buyLastBlock =
              buySellsLastBlocks.records.stream()
                  .filter(SingleBuySellRecord::isSuccess)
                  .filter(buy -> buy.isBuy)
                  .collect(Collectors.toList());

          TokenAllInfoRecord tokenAllInfoRecord =
              addrAllInfoRecord.getTokenAllInfoRecord(tokenEntry.getKey());
          boolean tokenBlockSuccess = buySellsLastBlocks.blockSuccess;

          // todo 抽方法
          Iterator<SingleBuySellRecord> sellRecordIterator = sellsLastBlock.iterator();
          while (sellRecordIterator.hasNext()) {
            SingleBuySellRecord sell = sellRecordIterator.next();

            Iterator<SingleBuySellRecord> buyRecordIterator = buyLastBlock.iterator();
            while (buyRecordIterator.hasNext()) {
              SingleBuySellRecord buy = buyRecordIterator.next();
              BigDecimal actualTokenAmount =
                  sell.getTokenAmount().compareTo(buy.getTokenAmount()) > 0
                      ? buy.getTokenAmount()
                      : sell.getTokenAmount();
              BigDecimal actualGetTrxAmount = sell.getActualTrxAmount(actualTokenAmount);
              BigDecimal actualOutTrxAmount = buy.getActualTrxAmount(actualTokenAmount);
              BigDecimal profit = actualGetTrxAmount.subtract(actualOutTrxAmount);
              if (profit.compareTo(BigDecimal.ZERO) > 0) {
                addrBlockSuccess = true;
              }
              addrAllInfoRecord.addTokenRecord(tokenEntry.getKey(), profit, true);

              buy.subTokenAmount(actualTokenAmount);
              sell.subTokenAmount(actualTokenAmount);
              if (!buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (!sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedLastBlock++;
              }

              if (buy.isMatched()) {
                buyRecordIterator.remove();
              }

              if (sell.isMatched()) {
                sellRecordIterator.remove();
                break;
              }
            }
          }

          // 上个块卖可以平账，最近2块有卖有卖
          //          if (!sellsLastBlock.isEmpty() && buySellsLastBlocks.remainingSellAvailable) {
          if (!sellsLastBlock.isEmpty()) {
            BigDecimal tokenAmount = BigDecimal.ZERO;
            BigDecimal trxAmount = BigDecimal.ZERO;
            for (SingleBuySellRecord sell : sellsLastBlock) {
              tokenAmount = tokenAmount.add(sell.getTokenAmount());
              trxAmount = trxAmount.add(sell.getTrxAmount());
            }
            // 平账
            tokenAllInfoRecord.removeRemaining(tokenAmount, trxAmount);
          }

          if (!buyLastBlock.isEmpty() && buySellsLastBlocks.remainingBuyAvailable) {
            // 全部匹配完还有剩余，记账
            BigDecimal tokenAmount = BigDecimal.ZERO;
            BigDecimal trxAmount = BigDecimal.ZERO;
            for (SingleBuySellRecord buy : buyLastBlock) {
              tokenAmount = tokenAmount.add(buy.getTokenAmount());
              trxAmount = trxAmount.add(buy.getTrxAmount());
            }
            // 记账
            tokenAllInfoRecord.addRemaining(tokenAmount, trxAmount);
          }

          long attackCountLastBlock =
              Math.min(
                  buySellsLastBlocks.availableAttackBuyCount(),
                  buySellsLastBlocks.availableAttackSellCount());
          tokenAllInfoRecord.addAttack(attackCountLastBlock);
          if (buySellsLastBlocks.attackTargetCount == 0 && buySellsLastBlocks.isAttacking()) {
            tokenAllInfoRecord.addAttackTarget(1);
            buySellsLastBlocks.attackTargetCount++;
            //            if (!buySellsLastBlocks.blockSuccess && tokenBlockSuccess) {
            //              tokenAllInfoRecord.addSuccessCount();
            //              buySellsLastBlocks.blockSuccess = true;
            //            }
          }

          // fee
          if (buyFeeAddedLastBlock < attackCountLastBlock) {
            for (SingleBuySellRecord buy : buySellsLastBlocks.records) {
              if (buy.isBuy && !buy.feeAdded) {
                feeToAdd = feeToAdd.add(buy.fee);
                buy.feeAdded = true;
                buyFeeAddedLastBlock++;
              }
              if (buyFeeAddedLastBlock == attackCountLastBlock) {
                break;
              }
            }
          }
          if (sellFeeAddedLastBlock < attackCountLastBlock) {
            for (SingleBuySellRecord sell : buySellsLastBlocks.records) {
              if (!sell.isBuy && !sell.feeAdded) {
                feeToAdd = feeToAdd.add(sell.fee);
                sell.feeAdded = true;
                sellFeeAddedLastBlock++;
              }
              if (sellFeeAddedLastBlock == attackCountLastBlock) {
                break;
              }
            }
          }
          // todo 整理
          if (buySellsLastBlocks.isAttacking()) {
            for (SingleBuySellRecord record : buySellsLastBlocks.records) {
              if (!record.feeAdded) {
                feeToAdd = feeToAdd.add(record.fee);
                record.feeAdded = true;
              }
            }
          }

          addrAllInfoRecord.updateTokenAllInfoRecord(tokenEntry.getKey(), tokenAllInfoRecord);
          addrAllInfoRecordMap.put(addr, addrAllInfoRecord);
        }
      }

      addrAllInfoRecord.addFee(feeToAdd);
      if (addrBlockSuccess && toRecordSr) {
        addrAllInfoRecord.addWitness(witness);
      }
      addrAllInfoRecordMap.put(addr, addrAllInfoRecord);
    }
    // 删除1个块之前记录
    for (Iterator<Map.Entry<String, AddrContinusRecord>> it =
            continusRecordMap.entrySet().iterator();
        it.hasNext(); ) {
      Map.Entry<String, AddrContinusRecord> entry = it.next();
      entry.getValue().removeRecordByBlockNum(blockNum - 1);
      if (entry.getValue().records.isEmpty()) {
        it.remove();
      }
    }
    return blockSuccess;
  }

  private static boolean matchBuySell(
      SingleBuySellRecord sellRecord,
      List<SingleBuySellRecord> buySells,
      AddrAllInfoRecord record,
      String token,
      int endIndex,
      boolean isLastBlock,
      boolean blockSuccess) {

    for (int i = 0; i < Math.min(endIndex, buySells.size()); i++) {
      SingleBuySellRecord toMatch = buySells.get(i);
      if (toMatch.isSuccess()
          && !toMatch.isMatched()
          && toMatch.isBuy
          && isMatch(toMatch.tokenAmount, sellRecord.tokenAmount)) {
        // 匹配上，计算获利和损失
        BigDecimal getTrx = sellRecord.trxAmount.subtract(toMatch.trxAmount);
        record.addTokenRecord(token, getTrx);
        toMatch.match();
        sellRecord.match();
        if (getTrx.compareTo(BigDecimal.ZERO) > 0 && !blockSuccess) {
          SingleBuySellRecord user = null;
          boolean flag = false;
          // 夹成功
          if (isLastBlock) {
            for (SingleBuySellRecord buy : buysLastBlock) {
              if (!buy.isMatched()
                  && buy.index > toMatch.index
                  //                  && buy.index < sellRecord.index
                  && buy.isSuccess()
                  && buy.isBuy
                  && buy.token.equalsIgnoreCase(token)) {
                buy.match();
                user = buy;
                flag = true;
                break;
              }
            }
          }
          if (!flag) {
            for (SingleBuySellRecord buy : buysThisBlock) {
              if (!buy.isMatched()
                  //                && buy.index > toMatch.index
                  && buy.index < sellRecord.index
                  && buy.isSuccess()
                  && buy.isBuy
                  && buy.token.equalsIgnoreCase(token)) {
                buy.match();
                user = buy;
                break;
              }
            }
          }
          writeToFile(toMatch, sellRecord, user);
        }
        return getTrx.compareTo(BigDecimal.ZERO) > 0;
      }
    }
    return blockSuccess;
  }

  private static void writeToFile(
      SingleBuySellRecord buy, SingleBuySellRecord sell, SingleBuySellRecord user) {
    if (user == null) {
      logger.info("Empty use tx, buy {} sell {}, token {}", buy.txId, sell.txId, buy.token);
    }
    mevWriter.println(
        writeId
            + " "
            + buy.txId
            + " "
            + sell.txId
            + " "
            + (user == null ? "null" : user.txId)
            + " "
            + (user == null ? "null" : user.caller)
            + " "
            + buy.blockNum
            + " "
            + buy.timestamp
            + " "
            + buy.witness);
    userWriter.println(
        writeId
            + " "
            + (user == null ? "null" : user.txId)
            + " "
            + (user == null ? "null" : user.caller)
            + " "
            + (user == null ? "null" : user.blockNum)
            + " "
            + (user == null ? "null" : user.timestamp)
            + " "
            + (user == null ? "null" : user.witness));
    writeId++;
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
    String txId;
    String caller;
    int index;
    String token;
    boolean isBuy;
    BigDecimal tokenAmount;
    BigDecimal trxAmount;
    long blockNum;
    long timestamp;
    String witness;
    boolean matched;
    boolean success;
    BigDecimal fee;
    boolean feeAdded;

    private SingleBuySellRecord(long blockNum, boolean success) {
      tokenAmount = BigDecimal.ZERO;
      trxAmount = BigDecimal.ZERO;
      this.blockNum = blockNum;
      matched = false;
      this.success = success;
      fee = BigDecimal.ZERO;
      feeAdded = false;
    }

    private BigDecimal getTokenPrice() {
      return trxAmount.divide(tokenAmount, 18, RoundingMode.HALF_UP);
    }

    private void match() {
      matched = true;
    }

    private BigDecimal getActualTrxAmount(BigDecimal actualTokenAmount) {
      if (actualTokenAmount.compareTo(tokenAmount) >= 0) {
        return trxAmount;
      }
      return trxAmount.multiply(actualTokenAmount).divide(tokenAmount, 6, RoundingMode.HALF_UP);
    }

    private void subTokenAmount(BigDecimal actualTokenAmount) {
      if (actualTokenAmount.compareTo(tokenAmount) >= 0
          || isMatch(actualTokenAmount, tokenAmount)) {
        tokenAmount = BigDecimal.ZERO;
        trxAmount = BigDecimal.ZERO;
        matched = true;
      } else {
        BigDecimal actualTrxAmount = getActualTrxAmount(actualTokenAmount);
        tokenAmount = tokenAmount.subtract(actualTokenAmount);
        trxAmount = trxAmount.subtract(actualTrxAmount);
      }
    }
  }

  @AllArgsConstructor
  @Getter
  private static class ContinusBlockRecord {
    List<SingleBuySellRecord> records;
    long buyCount;
    long sellCount;
    long recordBuyCount;
    long recordSellCount;
    boolean remainingSellAvailable;
    boolean remainingBuyAvailable;
    BigDecimal sellFeeRemaining;
    BigDecimal buyFeeRemaining;
    long attackTargetCount;
    boolean blockSuccess;

    private ContinusBlockRecord() {
      records = new ArrayList<>();
      buyCount = 0;
      sellCount = 0;
      recordBuyCount = 0;
      recordSellCount = 0;
      remainingSellAvailable = false;
      remainingBuyAvailable = false;
      sellFeeRemaining = BigDecimal.ZERO;
      buyFeeRemaining = BigDecimal.ZERO;
      attackTargetCount = 0;
      blockSuccess = false;
    }

    private void addRecord(SingleBuySellRecord singleBuySellRecord) {
      records.add(singleBuySellRecord);
    }

    private void clearAllSuccess() {
      records.removeIf(SingleBuySellRecord::isSuccess); // 清除成功的();
    }

    private void updateRecords(List<SingleBuySellRecord> newRecords) {
      records.clear();
      records.addAll(newRecords);
    }

    private BigDecimal getAllBuyTokenAmount() {
      return records.stream()
          .filter(record -> record.isBuy)
          .map(SingleBuySellRecord::getTokenAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getAllTrxOutAmount() {
      return records.stream()
          .filter(record -> record.isBuy)
          .map(SingleBuySellRecord::getTrxAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private void removeMatchedAndUnsuccess() {
      records.removeIf(record -> record.matched || !record.success);
    }

    private void removeMatched() {
      records.removeIf(record -> record.matched);
    }

    private void addBuyCount() {
      buyCount++;
    }

    private void addSellCount() {
      sellCount++;
    }

    private void addAttackTarget() {
      attackTargetCount++;
    }

    private void addSellCount(long count) {
      sellCount += count;
    }

    private long getBuyCount() {
      return buyCount;
    }

    private long getSellCount() {
      return sellCount;
    }

    private boolean hasSell() {
      return records.stream().anyMatch(record -> !record.isBuy);
    }

    private boolean hasSuccessSell() {
      return records.stream().anyMatch(record -> record.isSuccess() && !record.isBuy);
    }

    private boolean hasBuy() {
      return records.stream().anyMatch(record -> record.isBuy);
    }

    private long availableAttackBuyCount() {
      return buyCount - recordBuyCount;
    }

    private long availableAttackSellCount() {
      return sellCount - recordSellCount;
    }

    private void addUnsuccessFee() {
      for (SingleBuySellRecord record : records) {
        if (!record.success) {
          if (record.isBuy) {
            buyFeeRemaining = buyFeeRemaining.add(record.fee);
          } else {
            sellFeeRemaining = sellFeeRemaining.add(record.fee);
          }
        }
      }
    }

    private boolean isAttacking() {
      return buyCount > 0 && sellCount > 0;
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
        String txId,
        int index,
        long blockNum,
        long timestamp,
        String witness,
        String token,
        boolean isBuy,
        BigDecimal tokenAmount,
        BigDecimal trxAmount,
        boolean success,
        BigDecimal fee) {
      Map<String, ContinusBlockRecord> tokenContinusRecord =
          records.getOrDefault(blockNum, new HashMap<>());

      ContinusBlockRecord record =
          tokenContinusRecord.getOrDefault(token, new ContinusBlockRecord());
      // todo token
      SingleBuySellRecord singleBuySellRecord =
          new SingleBuySellRecord(
              txId,
              caller,
              index,
              null,
              isBuy,
              tokenAmount,
              trxAmount,
              blockNum,
              timestamp,
              witness,
              false,
              success,
              fee,
              false);
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
    long attackCount;
    long attackTargetCount;
    long mzsuccessCount;
    long mzlackCount;

    private TokenAllInfoRecord(String token) {
      this.token = token;
      profit = BigDecimal.ZERO;
      lack = BigDecimal.ZERO;
      successCount = 0;
      lackCount = 0;
      remainingTokenAmount = BigDecimal.ZERO;
      trxOutAmount = BigDecimal.ZERO;
      attackCount = 0;
      attackTargetCount = 0;
    }

    private void addTrxDiff(BigDecimal trxDiff, boolean mzzz) {
      if (trxDiff.compareTo(BigDecimal.ZERO) > 0) {
        profit = profit.add(trxDiff);
        //        if (mzzz) {
        //          mzsuccessCount++;
        //        } else {
        //          successCount++;
        //        }
      } else {
        lack = lack.add(trxDiff);
        //        if (mzzz) {
        //          mzlackCount++;
        //        } else {
        //          lackCount++;
        //        }
      }
    }

    private void addSuccessCount() {
      successCount++;
    }

    private void addRemaining(BigDecimal tokenAmount, BigDecimal trxOutAmount) {
      remainingTokenAmount = remainingTokenAmount.add(tokenAmount);
      this.trxOutAmount = this.trxOutAmount.add(trxOutAmount);
    }

    private void addAttack(long count) {
      attackCount += count;
    }

    private void addAttackTarget(long count) {
      attackTargetCount += count;
    }

    private void removeRemaining(BigDecimal tokenSellAmount, BigDecimal trxGetAmount) {
      BigDecimal actualTokenSellAmount =
          tokenSellAmount.compareTo(remainingTokenAmount) > 0
              ? remainingTokenAmount
              : tokenSellAmount;
      BigDecimal actualTrxGetAmount =
          trxGetAmount
              .multiply(actualTokenSellAmount)
              .divide(tokenSellAmount, 6, RoundingMode.HALF_EVEN);
      remainingTokenAmount = remainingTokenAmount.subtract(actualTokenSellAmount);
      trxOutAmount = trxOutAmount.subtract(actualTrxGetAmount);
    }
  }

  @AllArgsConstructor
  @Getter
  private static class AddrAllInfoRecord {
    String caller;
    long buyCount;
    BigDecimal fee;
    Map<String, TokenAllInfoRecord> tokenRecords; // <token, TokenAllInfoRecord>
    long successBuy;
    long successSell;
    long failBuy;
    long failSell;
    BigDecimal allfee;
    Map<String, Long> successSrBlock;

    private AddrAllInfoRecord(String caller) {
      this.caller = caller;
      buyCount = 0;
      tokenRecords = new HashMap<>();
      fee = BigDecimal.ZERO;
      successBuy = 0;
      successSell = 0;
      failBuy = 0;
      failSell = 0;
      allfee = BigDecimal.ZERO;
      successSrBlock = new HashMap<>();
    }

    private void addBuy() {
      buyCount++;
    }

    private void addTokenRecord(String token, BigDecimal trxDiff) {
      addTokenRecord(token, trxDiff, false);
    }

    private void addTokenRecord(String token, BigDecimal trxDiff, boolean mzzz) {

      TokenAllInfoRecord tokenAllInfoRecord =
          tokenRecords.getOrDefault(token, new TokenAllInfoRecord(token));
      tokenAllInfoRecord.addTrxDiff(trxDiff, mzzz);
      tokenRecords.put(token, tokenAllInfoRecord);
    }

    private TokenAllInfoRecord getTokenAllInfoRecord(String token) {
      return tokenRecords.getOrDefault(token, new TokenAllInfoRecord(token));
    }

    private void updateTokenAllInfoRecord(String token, TokenAllInfoRecord tokenAllInfoRecord) {
      tokenRecords.put(token, tokenAllInfoRecord);
    }

    private BigDecimal getAllProfit() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getProfit)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getAllLack() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getLack)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private long getAllAttack() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getAttackCount).sum();
    }

    private long getAllAttackTarget() {
      return tokenRecords.values().stream()
          .mapToLong(TokenAllInfoRecord::getAttackTargetCount)
          .sum();
    }

    private long getSuccessCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getSuccessCount).sum();
    }

    private long getLackCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getLackCount).sum();
    }

    private BigDecimal getRemainingTokenAmount() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getRemainingTokenAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private BigDecimal getTrxOutAmount() {
      return tokenRecords.values().stream()
          .map(TokenAllInfoRecord::getTrxOutAmount)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private long getMzSucCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getMzsuccessCount).sum();
    }

    private long getMzLackCount() {
      return tokenRecords.values().stream().mapToLong(TokenAllInfoRecord::getMzlackCount).sum();
    }

    private BigDecimal addFee(BigDecimal newfee) {
      return this.fee = this.fee.add(newfee);
    }

    private void addWitness(String witness) {
      successSrBlock.put(witness, successSrBlock.getOrDefault(witness, 0L) + 1);
    }
  }

  private static boolean isMatch(BigDecimal first, BigDecimal second) {
    BigDecimal difference = first.subtract(second).abs(); // 计算差的绝对值
    BigDecimal threshold = new BigDecimal("1"); // 设置阈值为 1

    // 比较绝对值是否小于 1
    return difference.compareTo(threshold) <= 0;
  }

  private static final byte[] USDT_ADDR = Hex.decode("41a614f803B6FD780986A42c78Ec9c7f77e6DeD13C");
  private static final byte[] SWAP_ADDR = Hex.decode("416E0617948FE030a7E4970f8389d4Ad295f249B7e");
  private static final byte[] PUMP_ADDR_1 =
      Hex.decode("41B7bc8fc76aF164029c27d4104b09f83b607e45bB");
  private static final byte[] PUMP_ADDR_2 =
      Hex.decode("41C22DD1b7Bc7574e94563C8282F64B065bC07b2fa");
  private static final byte[] PUMP_ADDR_3 =
      Hex.decode("41fF7155b5df8008fbF3834922B2D52430b27874f5");

  private static void syncEnergy() {
    try {
      logger.info("Start energy task ...");
      // pump
      // TSiiYf1b1PV1fpT9T7V4wy11btsNVajw1g
      // TTfvyrAz86hbZk5iDpKD78pqLGgi8C7AAw
      // TZFs5ch1R1C4mmjwrrmZqeqbUgGpxY1yWB
      // sunswap v2 TKzxdSv2FZKQrEqkKVgp5DcwEXBEKMg2Ax
      //      Set<String> pumpAddrs =
      //          Stream.of(
      //                  "TSiiYf1b1PV1fpT9T7V4wy11btsNVajw1g",
      //                  "TTfvyrAz86hbZk5iDpKD78pqLGgi8C7AAw",
      //                  "TZFs5ch1R1C4mmjwrrmZqeqbUgGpxY1yWB")
      //              .map(s -> get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(s))))
      //              .collect(Collectors.toSet());
      //      Set<String> swapAddrs =
      //          Stream.of("TKzxdSv2FZKQrEqkKVgp5DcwEXBEKMg2Ax")
      //              .map(s -> get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(s))))
      //              .collect(Collectors.toSet());
      Map<String, Set<String>> cexAddrs = getTronCexAddresses();
      //      cexAddrs
      //          .get("Bybit")
      //          .add(
      //              get41Addr(
      //                  Hex.toHexString(
      //
      // Commons.decodeFromBase58Check("TZBAacgmAa84RPwa1aSWL6eMc7B9GmBDrK"))));
      //      Set<String> allCexAddrs =
      //          cexAddrs.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
      //      cexAddrs.remove("Others");
      // todo
      long firstDayStartBlock = 65337555;
      long secondDayStartBlock = 65366347;
      long thirdDayStartBlock = 65395139;
      long thirdDayEndBlock = 0;

      //      long latestBlockNum = ChainBaseManager.getInstance().getHeadBlockNum();
      //      long lowestBlockNum = ChainBaseManager.getInstance().getLowestBlockNum();
      // 08-16 08:00
      //      firstDayStartBlock = 64366247;
      //      // 08-16 14:00
      //      secondDayStartBlock = 64373445;
      //      // 08-16 20:00
      //      thirdDayStartBlock = 64380643;
      //      // 08-16 20:00
      //      thirdDayEndBlock = 64387840;
      // 总的充币地址
      Map<String, Set<String>> chargeAddrs =
          getChargeAddrsV2(
              "All",
              cexAddrs.get("Binance"),
              cexAddrs.get("Okex"),
              cexAddrs.get("Bybit"),
              cexAddrs,
              thirdDayStartBlock,
              thirdDayEndBlock);
      //      for (Map.Entry<String, Set<String>> entry : cexAddrs.entrySet()) {
      //        if (!entry.getKey().equalsIgnoreCase("Others")) {
      //          chargeAddrs.put(
      //              entry.getKey(),
      //              getChargeAddrsV2(
      //                  entry.getKey(),
      //                  entry.getValue(),
      //                  allCexAddrs,
      //                  firstDayStartBlock - 7 * 28800,
      //                  thirdDayEndBlock));
      //        }
      //      }

      File file = new File("binance1.txt");
      if (file.exists()) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Set<String> binanceChargers = chargeAddrs.get("Binance");
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("T")) {
            binanceChargers.add(get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(line))));
          }
        }
        chargeAddrs.put("Binance", binanceChargers);
      }
      file = new File("binance2.txt");
      if (file.exists()) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Set<String> binanceChargers = chargeAddrs.get("Binance");
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("T")) {
            binanceChargers.add(get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(line))));
          }
        }
        chargeAddrs.put("Binance", binanceChargers);
      }
      file = new File("okex.txt");
      if (file.exists()) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Set<String> okexChargers = chargeAddrs.get("Okex");
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("T")) {
            okexChargers.add(get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(line))));
          }
        }
        chargeAddrs.put("Okex", okexChargers);
      }

      file = new File("bybit1.txt");
      if (file.exists()) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Set<String> bybitChargers = chargeAddrs.get("Bybit");
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("T")) {
            bybitChargers.add(get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(line))));
          }
        }
        chargeAddrs.put("Bybit", bybitChargers);
      }
      file = new File("bybit2.txt");
      if (file.exists()) {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Set<String> bybitChargers = chargeAddrs.get("Bybit");
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("T")) {
            bybitChargers.add(get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(line))));
          }
        }
        chargeAddrs.put("Bybit", bybitChargers);
      }

      logger.info(
          "Get cex charge addrs success, Binance {}, Okex {}, Bybit {}, start sync energy data ...",
          chargeAddrs.get("Binance").size(),
          chargeAddrs.get("Okex").size(),
          chargeAddrs.get("Bybit").size());
      StringBuilder res = new StringBuilder();

      Map<String, Set<String>> originCexAddrs = new HashMap<>(cexAddrs);
      cexAddrs.remove("Others");
      //      if (secondDayStartBlock > 0) {
      //        EnergyRecord firstDay =
      //            syncOneDayEnergy(
      //                cexAddrs, chargeAddrs, originCexAddrs, firstDayStartBlock,
      // secondDayStartBlock - 1);
      //        res.append("\n").append(getRecordMsg(firstDay));
      //      }
      //      if (thirdDayStartBlock > 0) {
      EnergyRecord secondDay =
          syncOneDayEnergy(cexAddrs, chargeAddrs, originCexAddrs, 65467119, 65481514);
      res.append("\n").append(getRecordMsg(secondDay));
      //      }
      //      if (thirdDayEndBlock > 0) {
      EnergyRecord thirdDay =
          syncOneDayEnergy(cexAddrs, chargeAddrs, originCexAddrs, 65452723, 65481514);
      res.append("\n").append(getRecordMsg(thirdDay));
      //      }

      logger.info(res.toString());
      System.out.println(res);
      logger.info("Energy task end!!!");
    } catch (Exception e) {
      logger.error("Sync energy error", e);
    }
  }

  private static EnergyRecord syncOneDayEnergy(
      Map<String, Set<String>> cexAddrs,
      Map<String, Set<String>> chargeAddrs,
      Map<String, Set<String>> originCexAddrs,
      long startBlockNum,
      long endBlockNum)
      throws BadItemException {
    logger.info("Start syncing one day energy from {} to {}", startBlockNum, endBlockNum);
    Set<String> allCexAddrs =
        originCexAddrs.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    // get energy record
    DBIterator retIterator =
        (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
    retIterator.seek(ByteArray.fromLong(startBlockNum));
    DBIterator blockIterator =
        (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
    blockIterator.seek(ByteArray.fromLong(startBlockNum));

    EnergyRecord record = new EnergyRecord();
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
      if (blockNum > endBlockNum) {
        break;
      }

      byte[] value = retEntry.getValue();
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
      BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
      Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
      for (TransactionCapsule tx : blockCapsule.getTransactions()) {
        txCallerMap.put(tx.getTransactionId().toString(), tx);
      }

      for (Protocol.TransactionInfo transactionInfo :
          transactionRetCapsule.getInstance().getTransactioninfoList()) {
        byte[] txId = transactionInfo.getId().toByteArray();
        // toodo remove
        String txHash = Hex.toHexString(txId);
        TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));

        if (tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.TriggerSmartContract.class)
            || tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.CreateSmartContract.class)) {
          byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
          long fee = transactionInfo.getReceipt().getEnergyFee();
          long energyCost = transactionInfo.getReceipt().getEnergyUsageTotal();
          long burnEnergy =
              transactionInfo.getReceipt().getEnergyUsageTotal()
                  - transactionInfo.getReceipt().getEnergyUsage()
                  - transactionInfo.getReceipt().getOriginEnergyUsage();
          // 全网
          record.addMainnetRecord(energyCost, burnEnergy, fee);

          // todo remove success
          if (Arrays.equals(contractAddress, USDT_ADDR)) {
            try {
              StringBuilder calldata =
                  new StringBuilder(
                      Hex.toHexString(
                          tx.getInstance()
                              .getRawData()
                              .getContract(0)
                              .getParameter()
                              .unpack(SmartContractOuterClass.TriggerSmartContract.class)
                              .getData()
                              .toByteArray()));
              if (calldata.toString().startsWith("a9059cbb")
                  || calldata.toString().startsWith("23b872dd")) {
                String fromAddress;
                String toAddress;
                //                BigInteger amount;
                if (calldata.toString().startsWith("a9059cbb")) {
                  fromAddress = get41Addr(Hex.toHexString(tx.getOwnerAddress()));
                  toAddress = "41" + calldata.substring(32, 36 * 2);
                  //                  if (calldata.length() < 136) {
                  //                    amount = BigInteger.valueOf(0);
                  //                  } else {
                  //                    amount = new BigInteger(calldata.substring(36 * 2, 68 * 2),
                  // 16);
                  //                  }
                } else {
                  fromAddress = "41" + calldata.substring(32, 36 * 2);
                  if (calldata.length() < 136) {
                    int appendLen = 136 - calldata.length();
                    for (int i = 0; i < appendLen; i++) {
                      calldata.append("0");
                    }
                  }
                  toAddress = "41" + calldata.substring(32 * 3, 68 * 2);
                  //                  if (calldata.length() < 200) {
                  //                    amount = BigInteger.valueOf(0);
                  //                  } else {
                  //                    amount = new BigInteger(calldata.substring(68 * 2, 100 * 2),
                  // 16);
                  //                  }
                }

                //                cexAddrs.forEach(
                //                    (k, v) -> {
                //                      Set<String> curChargeAddrs = chargeAddrs.get(k);
                //                      if (curChargeAddrs.contains(toAddress)) {
                //                        // 充币
                //                        if (k.equalsIgnoreCase("Binance")) {
                //                          record.addBinanceChargeRecord(energyCost, burnEnergy,
                // fee);
                //                        } else if (k.equalsIgnoreCase("Bybit")) {
                //                          record.addBybitChargeRecord(energyCost, burnEnergy,
                // fee);
                //                        } else if (k.equalsIgnoreCase("Okex")) {
                //                          record.addOkChargeRecord(energyCost, burnEnergy, fee);
                //                        }
                //                      } else if (v.contains(toAddress)) {
                //                        // 归集
                //                        if (k.equalsIgnoreCase("Binance")) {
                //                          record.addBinanceCollectRecord(energyCost, burnEnergy,
                // fee);
                //                        } else if (k.equalsIgnoreCase("Bybit")) {
                //                          record.addBybitCollectRecord(energyCost, burnEnergy,
                // fee);
                //                        } else if (k.equalsIgnoreCase("Okex")) {
                //                          record.addOkCollectRecord(energyCost, burnEnergy, fee);
                //                        }
                //                      } else if (v.contains(fromAddress)) {
                //                        // 提币
                //                        if (k.equalsIgnoreCase("Binance")) {
                //                          record.addBinanceWithdrawRecord(energyCost, burnEnergy,
                // fee);
                //                        } else if (k.equalsIgnoreCase("Bybit")) {
                //                          record.addBybitWithdrawRecord(energyCost, burnEnergy,
                // fee);
                //                        } else if (k.equalsIgnoreCase("Okex")) {
                //                          record.addOkWithdrawRecord(energyCost, burnEnergy, fee);
                //                        }
                //                      }
                //                    });

                for (Map.Entry<String, Set<String>> entry : cexAddrs.entrySet()) {
                  String cexName = entry.getKey();
                  Set<String> curCexAddrs = entry.getValue();
                  Set<String> curChargeAddrs = chargeAddrs.get(cexName);
                  if (!curCexAddrs.contains(fromAddress) && curChargeAddrs.contains(toAddress)) {
                    //                  amount.compareTo(new BigInteger("500000")) > 0;
                    // todo amount > 0.5
                    // 充币
                    if (cexName.equalsIgnoreCase("Binance")) {
                      record.addBinanceChargeRecord(energyCost, burnEnergy, fee);
                    } else if (cexName.equalsIgnoreCase("Bybit")) {
                      record.addBybitChargeRecord(energyCost, burnEnergy, fee);
                    } else if (cexName.equalsIgnoreCase("Okex")) {
                      record.addOkChargeRecord(energyCost, burnEnergy, fee);
                    }

                  } else if (curChargeAddrs.contains(fromAddress)
                      && curCexAddrs.contains(toAddress)) {
                    // 归集
                    if (cexName.equalsIgnoreCase("Binance")) {
                      record.addBinanceCollectRecord(energyCost, burnEnergy, fee);
                    } else if (cexName.equalsIgnoreCase("Bybit")) {
                      record.addBybitCollectRecord(energyCost, burnEnergy, fee);
                    } else if (cexName.equalsIgnoreCase("Okex")) {
                      record.addOkCollectRecord(energyCost, burnEnergy, fee);
                    }

                  } else if (curCexAddrs.contains(fromAddress)) {
                    // 提币
                    if (cexName.equalsIgnoreCase("Binance")) {
                      record.addBinanceWithdrawRecord(energyCost, burnEnergy, fee);
                    } else if (cexName.equalsIgnoreCase("Bybit")) {
                      record.addBybitWithdrawRecord(energyCost, burnEnergy, fee);
                    } else if (cexName.equalsIgnoreCase("Okex")) {
                      record.addOkWithdrawRecord(energyCost, burnEnergy, fee);
                    }
                  }
                }
              }
            } catch (Exception e) {
              //              throw new RuntimeException(e);
              //              logger.error(txHash);
            }
          } else if (Arrays.equals(contractAddress, SWAP_ADDR)) {
            record.addSwapRecord(energyCost, burnEnergy, fee);

          } else if (Arrays.equals(contractAddress, PUMP_ADDR_1)
              || Arrays.equals(contractAddress, PUMP_ADDR_2)
              || Arrays.equals(contractAddress, PUMP_ADDR_3)) {
            record.addPumpRecord(energyCost, burnEnergy, fee);
          }
        }
      }
    }

    logger.info("End syncing one day energy from {} to {} !!!", startBlockNum, endBlockNum);
    return record;
  }

  private static Map<String, Set<String>> getChargeAddrs(
      Map<String, Set<String>> cexAddrs, long startBlockNum, long endBlockNum)
      throws BadItemException {

    DBIterator retIterator =
        (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
    retIterator.seek(ByteArray.fromLong(startBlockNum));
    DBIterator blockIterator =
        (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
    blockIterator.seek(ByteArray.fromLong(startBlockNum));
    Map<String, Set<String>> chargeAddrs = new HashMap<>();
    long logBlockNum = startBlockNum;

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
      if (blockNum > endBlockNum) {
        break;
      }

      byte[] value = retEntry.getValue();
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
      BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
      Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
      for (TransactionCapsule tx : blockCapsule.getTransactions()) {
        txCallerMap.put(tx.getTransactionId().toString(), tx);
      }

      for (Protocol.TransactionInfo transactionInfo :
          transactionRetCapsule.getInstance().getTransactioninfoList()) {
        byte[] txId = transactionInfo.getId().toByteArray();
        TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));
        byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
        if (Arrays.equals(contractAddress, USDT_ADDR)
            && transactionInfo.getResult().equals(SUCESS)) {
          try {
            StringBuilder calldata =
                new StringBuilder(
                    Hex.toHexString(
                        tx.getInstance()
                            .getRawData()
                            .getContract(0)
                            .getParameter()
                            .unpack(SmartContractOuterClass.TriggerSmartContract.class)
                            .getData()
                            .toByteArray()));
            if (calldata.toString().startsWith("a9059cbb")
                || calldata.toString().startsWith("23b872dd")) {
              String fromAddress;
              String toAddress;
              if (calldata.toString().startsWith("a9059cbb")) {
                fromAddress = get41Addr(Hex.toHexString(tx.getOwnerAddress()));
                toAddress = "41" + calldata.substring(32, 36 * 2);
              } else {
                fromAddress = "41" + calldata.substring(32, 36 * 2);
                if (calldata.length() < 136) {
                  int appendLen = 136 - calldata.length();
                  for (int i = 0; i < appendLen; i++) {
                    calldata.append("0");
                  }
                }
                toAddress = "41" + calldata.substring(32 * 3, 68 * 2);
              }

              cexAddrs.forEach(
                  (k, v) -> {
                    if (v.contains(toAddress) && !v.contains(fromAddress)) {
                      Set<String> curChargeAddrs = chargeAddrs.getOrDefault(k, new HashSet<>());
                      curChargeAddrs.add(fromAddress);
                      chargeAddrs.put(k, curChargeAddrs);
                    }
                  });
            }
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        }
      }
      if (blockNum - logBlockNum >= 10000) {
        logBlockNum = blockNum;
        logger.info(
            "Getting charge addrs to timestamp {}, Binance {}, Okex {}, Bybit {}",
            transactionRetCapsule.getInstance().getBlockTimeStamp(),
            chargeAddrs.get("Binance").size(),
            chargeAddrs.get("Okex").size(),
            chargeAddrs.get("Bybit").size());
      }
    }
    //
    //    chargeAddrs.forEach(
    //        (k, v) -> {
    //          for (Iterator<Map.Entry<String, Set<String>>> it = v.entrySet().iterator();
    //              it.hasNext(); ) {
    //            Map.Entry<String, Set<String>> entry = it.next();
    //            if (entry.getValue().size() >= 3) {
    //              it.remove();
    //            }
    //          }
    //        });
    //
    //    Map<String, Set<String>> res = new HashMap<>();
    //    chargeAddrs.forEach(
    //        (k, v) -> {
    //          Set<String> chargeAds = new HashSet<>(v.keySet());
    //          res.put(k, chargeAds);
    //        });
    return chargeAddrs;
  }

  private static Map<String, Set<String>> getChargeAddrsV2(
      String cexName,
      Set<String> binanceAddrs,
      Set<String> okAddrs,
      Set<String> bybitAddrs,
      Map<String, Set<String>> cexAddrs,
      long startBlockNum,
      long endBlockNum)
      throws BadItemException {

    Set<String> allCexAddrs =
        cexAddrs.values().stream().flatMap(Set::stream).collect(Collectors.toSet());

    DBIterator retIterator =
        (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
    retIterator.seek(ByteArray.fromLong(startBlockNum));
    DBIterator blockIterator =
        (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
    blockIterator.seek(ByteArray.fromLong(startBlockNum));
    Map<String, Set<String>> binanceInterAddrs = new HashMap<>();
    Map<String, Set<String>> okInterAddrs = new HashMap<>();
    Map<String, Set<String>> bybitInterAddrs = new HashMap<>();
    long logBlockNum = startBlockNum;

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
      if (blockNum > endBlockNum) {
        break;
      }

      byte[] value = retEntry.getValue();
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
      BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
      Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
      for (TransactionCapsule tx : blockCapsule.getTransactions()) {
        txCallerMap.put(tx.getTransactionId().toString(), tx);
      }

      for (Protocol.TransactionInfo transactionInfo :
          transactionRetCapsule.getInstance().getTransactioninfoList()) {
        byte[] txId = transactionInfo.getId().toByteArray();
        TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));
        byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
        if (Arrays.equals(contractAddress, USDT_ADDR)
            && transactionInfo.getResult().equals(SUCESS)) {
          try {
            StringBuilder calldata =
                new StringBuilder(
                    Hex.toHexString(
                        tx.getInstance()
                            .getRawData()
                            .getContract(0)
                            .getParameter()
                            .unpack(SmartContractOuterClass.TriggerSmartContract.class)
                            .getData()
                            .toByteArray()));
            if (calldata.toString().startsWith("a9059cbb")
                || calldata.toString().startsWith("23b872dd")) {
              String fromAddress;
              String toAddress;
              if (calldata.toString().startsWith("a9059cbb")) {
                fromAddress = get41Addr(Hex.toHexString(tx.getOwnerAddress()));
                toAddress = "41" + calldata.substring(32, 36 * 2);
              } else {
                fromAddress = "41" + calldata.substring(32, 36 * 2);
                if (calldata.length() < 136) {
                  int appendLen = 136 - calldata.length();
                  for (int i = 0; i < appendLen; i++) {
                    calldata.append("0");
                  }
                }
                toAddress = "41" + calldata.substring(32 * 3, 68 * 2);
              }

              if (!allCexAddrs.contains(fromAddress)) {
                if (binanceAddrs.contains(toAddress)) {
                  Set<String> interAds =
                      binanceInterAddrs.getOrDefault(fromAddress, new HashSet<>());
                  interAds.add("Binance");
                  binanceInterAddrs.put(fromAddress, interAds);
                } else if (okAddrs.contains(toAddress)) {
                  Set<String> interAds = okInterAddrs.getOrDefault(fromAddress, new HashSet<>());
                  interAds.add("Okex");
                  okInterAddrs.put(fromAddress, interAds);
                } else if (bybitAddrs.contains(toAddress)) {
                  Set<String> interAds = bybitInterAddrs.getOrDefault(fromAddress, new HashSet<>());
                  interAds.add("Bybit");
                  bybitInterAddrs.put(fromAddress, interAds);
                }
              }
            }
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        }
      }
      if (blockNum - logBlockNum >= 10000) {
        logBlockNum = blockNum;
        logger.info(
            "Getting {} charge addrs to timestamp {}, binance {}, okex {}, bybit {}",
            cexName,
            transactionRetCapsule.getInstance().getBlockTimeStamp(),
            binanceInterAddrs.keySet().size(),
            okInterAddrs.keySet().size(),
            bybitInterAddrs.keySet().size());
      }
    }

    // filter
    retIterator =
        (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
    retIterator.seek(ByteArray.fromLong(startBlockNum));
    blockIterator = (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
    blockIterator.seek(ByteArray.fromLong(startBlockNum));
    logBlockNum = startBlockNum;

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
      if (blockNum > endBlockNum) {
        break;
      }

      byte[] value = retEntry.getValue();
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
      BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
      Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
      for (TransactionCapsule tx : blockCapsule.getTransactions()) {
        txCallerMap.put(tx.getTransactionId().toString(), tx);
      }

      for (Protocol.TransactionInfo transactionInfo :
          transactionRetCapsule.getInstance().getTransactioninfoList()) {
        byte[] txId = transactionInfo.getId().toByteArray();
        TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));
        byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
        if (Arrays.equals(contractAddress, USDT_ADDR)
            && transactionInfo.getResult().equals(SUCESS)) {
          try {
            StringBuilder calldata =
                new StringBuilder(
                    Hex.toHexString(
                        tx.getInstance()
                            .getRawData()
                            .getContract(0)
                            .getParameter()
                            .unpack(SmartContractOuterClass.TriggerSmartContract.class)
                            .getData()
                            .toByteArray()));
            if (calldata.toString().startsWith("a9059cbb")
                || calldata.toString().startsWith("23b872dd")) {
              String fromAddress;
              String toAddress;
              if (calldata.toString().startsWith("a9059cbb")) {
                fromAddress = get41Addr(Hex.toHexString(tx.getOwnerAddress()));
                toAddress = "41" + calldata.substring(32, 36 * 2);
              } else {
                fromAddress = "41" + calldata.substring(32, 36 * 2);
                if (calldata.length() < 136) {
                  int appendLen = 136 - calldata.length();
                  for (int i = 0; i < appendLen; i++) {
                    calldata.append("0");
                  }
                }
                toAddress = "41" + calldata.substring(32 * 3, 68 * 2);
              }

              if (binanceInterAddrs.containsKey(fromAddress)) {
                if (!cexAddrs.get("Binance").contains(toAddress)) {
                  Set<String> curInterAddrs =
                      binanceInterAddrs.getOrDefault(fromAddress, new HashSet<>());
                  String toAdd = toAddress;
                  if (cexAddrs.get("Okex").contains(toAddress)) {
                    toAdd = "Okex";
                  } else if (cexAddrs.get("Bybit").contains(toAddress)) {
                    toAdd = "Bybit";
                  } else if (cexAddrs.get("Others").contains(toAddress)) {
                    toAdd = "Others";
                  }
                  if (curInterAddrs.size() < 3) {
                    curInterAddrs.add(toAdd);
                  }
                  binanceInterAddrs.put(fromAddress, curInterAddrs);
                }
              }

              if (okInterAddrs.containsKey(fromAddress)) {
                if (!cexAddrs.get("Okex").contains(toAddress)) {
                  Set<String> curInterAddrs =
                      okInterAddrs.getOrDefault(fromAddress, new HashSet<>());
                  String toAdd = toAddress;
                  if (cexAddrs.get("Binance").contains(toAddress)) {
                    toAdd = "Binance";
                  } else if (cexAddrs.get("Bybit").contains(toAddress)) {
                    toAdd = "Bybit";
                  } else if (cexAddrs.get("Others").contains(toAddress)) {
                    toAdd = "Others";
                  }
                  if (curInterAddrs.size() < 3) {
                    curInterAddrs.add(toAdd);
                  }
                  okInterAddrs.put(fromAddress, curInterAddrs);
                }
              }
              if (bybitInterAddrs.containsKey(fromAddress)) {
                if (!cexAddrs.get("Bybit").contains(toAddress)) {
                  Set<String> curInterAddrs =
                      bybitInterAddrs.getOrDefault(fromAddress, new HashSet<>());
                  String toAdd = toAddress;
                  if (cexAddrs.get("Okex").contains(toAddress)) {
                    toAdd = "Okex";
                  } else if (cexAddrs.get("Binance").contains(toAddress)) {
                    toAdd = "Binance";
                  } else if (cexAddrs.get("Others").contains(toAddress)) {
                    toAdd = "Others";
                  }
                  if (curInterAddrs.size() < 3) {
                    curInterAddrs.add(toAdd);
                  }
                  bybitInterAddrs.put(fromAddress, curInterAddrs);
                }
              }
            }
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
          }
        }
      }
      if (blockNum - logBlockNum >= 10000) {
        logBlockNum = blockNum;
        logger.info(
            "Filtering {} charge addrs to timestamp {}",
            cexName,
            transactionRetCapsule.getInstance().getBlockTimeStamp());
      }
    }

    binanceInterAddrs
        .entrySet()
        .removeIf(
            entry ->
                entry.getValue().size() >= 3
                    || entry.getValue().contains("Okex")
                    || entry.getValue().contains("Bybit")
                    || entry.getValue().contains("Others"));
    okInterAddrs
        .entrySet()
        .removeIf(
            entry ->
                entry.getValue().size() >= 3
                    || entry.getValue().contains("Binance")
                    || entry.getValue().contains("Bybit")
                    || entry.getValue().contains("Others"));
    bybitInterAddrs
        .entrySet()
        .removeIf(
            entry ->
                entry.getValue().size() >= 3
                    || entry.getValue().contains("Binance")
                    || entry.getValue().contains("Okex")
                    || entry.getValue().contains("Others"));

    Map<String, Set<String>> res = new HashMap<>();
    res.put("Binance", new HashSet<>(binanceInterAddrs.keySet()));
    res.put("Okex", new HashSet<>(okInterAddrs.keySet()));
    res.put("Bybit", new HashSet<>(bybitInterAddrs.keySet()));

    allCexAddrs.forEach(cexAd -> res.values().forEach(set -> set.remove(cexAd)));

    return res;
  }

  private static Map<String, Set<String>> getTronCexAddresses() {
    try {
      JSONObject resObject =
          JSONObject.parseObject(NetUtil.get("https://apilist.tronscanapi.com/api/hot/exchanges"));

      Map<String, Set<String>> res = new HashMap<>();

      resObject
          .getJSONArray("exchanges")
          .forEach(
              obj -> {
                JSONObject jo = (JSONObject) obj;
                String addr = jo.getString("address");
                String name = jo.getString("name");
                String cexName = null;
                if (name.contains("binance") || name.contains("Binance")) {
                  cexName = "Binance";
                } else if (name.contains("Okex") || name.contains("okex")) {
                  cexName = "Okex";
                } else if (name.contains("bybit") || name.contains("Bybit")) {
                  cexName = "Bybit";
                } else {
                  cexName = "Others";
                }
                Set<String> addrs = res.getOrDefault(cexName, new HashSet<>());
                addrs.add(get41Addr(Hex.toHexString(Commons.decodeFromBase58Check(addr))));
                res.put(cexName, addrs);
              });
      return res;
    } catch (Exception e) {
      logger.error("Stat task getTronCexAddresses error, {}", e.getMessage());
      return new HashMap<>();
    }
  }

  private static long getBlockByTimestamp(long timestamp) throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      try {
        JSONObject res =
            JSONObject.parseObject(
                NetUtil.get(
                    "https://apilist.tronscanapi.com/api/block?limit=1&end_timestamp="
                        + timestamp));
        return res.getJSONArray("data").getJSONObject(0).getLongValue("number");
      } catch (Exception ex) {
        logger.info("getBlockByTimestamp error ", ex);
        Thread.sleep(2000);
      }
    }
    return -1;
  }

  @AllArgsConstructor
  @Getter
  private static class EnergyRecord {
    private SingleEnergyRecord mainnetRecord;
    private SingleEnergyRecord pumpRecord;
    private SingleEnergyRecord swapRecord;
    private CexRecord binanceRecord;
    private CexRecord bybitRecord;
    private CexRecord okRecord;

    private EnergyRecord() {
      this.mainnetRecord = new SingleEnergyRecord();
      this.pumpRecord = new SingleEnergyRecord();
      this.swapRecord = new SingleEnergyRecord();
      this.binanceRecord = new CexRecord();
      this.bybitRecord = new CexRecord();
      this.okRecord = new CexRecord();
    }

    private void addPumpRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      pumpRecord.addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addSwapRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      swapRecord.addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addMainnetRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      mainnetRecord.addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addBinanceChargeRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      binanceRecord.getChargeRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addBinanceWithdrawRecord(
        long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      binanceRecord.getWithdrawRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addBinanceCollectRecord(
        long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      binanceRecord.getCollectRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addOkChargeRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      okRecord.getChargeRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addOkWithdrawRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      okRecord.getWithdrawRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addOkCollectRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      okRecord.getCollectRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addBybitChargeRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      bybitRecord.getChargeRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addBybitWithdrawRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      bybitRecord.getWithdrawRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addBybitCollectRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      bybitRecord.getCollectRecord().addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }
  }

  @AllArgsConstructor
  @Getter
  private static class SingleEnergyRecord {
    long txCount;

    long energyCost;

    long burnEnergy;

    long fee;

    private SingleEnergyRecord() {
      this.txCount = 0;
      this.energyCost = 0;
      this.burnEnergy = 0;
      this.fee = 0;
    }

    private void addRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      this.txCount++;
      this.energyCost += energyCostToAdd;
      this.burnEnergy += burnEnergyToAdd;
      this.fee += feeToAdd;
    }
  }

  @AllArgsConstructor
  @Getter
  private static class CexRecord {
    SingleEnergyRecord collectRecord;
    SingleEnergyRecord withdrawRecord;
    SingleEnergyRecord chargeRecord;

    private CexRecord() {
      this.collectRecord = new SingleEnergyRecord();
      this.withdrawRecord = new SingleEnergyRecord();
      this.chargeRecord = new SingleEnergyRecord();
    }

    private void addCollectRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      this.collectRecord.addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addWithdrawRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      this.withdrawRecord.addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private void addChargeRecord(long energyCostToAdd, long burnEnergyToAdd, long feeToAdd) {
      this.chargeRecord.addRecord(energyCostToAdd, burnEnergyToAdd, feeToAdd);
    }

    private long getAllTxCount() {
      return chargeRecord.getTxCount() + withdrawRecord.getTxCount() + collectRecord.getTxCount();
    }

    private long getAllEnergyCost() {
      return chargeRecord.getEnergyCost()
          + withdrawRecord.getEnergyCost()
          + collectRecord.getEnergyCost();
    }

    private long getAllBurnEnergy() {
      return chargeRecord.getBurnEnergy()
          + withdrawRecord.getBurnEnergy()
          + collectRecord.getBurnEnergy();
    }

    private long getAllFee() {
      return chargeRecord.getFee() + withdrawRecord.getFee() + collectRecord.getFee();
    }
  }

  private static String getRecordMsg(EnergyRecord record) {
    return getCexRecordString(record.getBinanceRecord())
        + " "
        + getCexRecordString(record.getOkRecord())
        + " "
        + getCexRecordString(record.getBybitRecord())
        + " "
        + getSingleRecordString(record.getPumpRecord())
        + " "
        + getSingleRecordString(record.getSwapRecord())
        + " "
        + getSingleRecordString(record.getMainnetRecord());
  }

  private static String getCexRecordString(CexRecord record) {
    return record.getAllTxCount()
        + " "
        + record.getAllEnergyCost()
        + " "
        + record.getAllBurnEnergy()
        + " "
        + ((double) record.getAllBurnEnergy() / record.getAllEnergyCost())
        + " "
        + ((double) record.getAllFee() / 1000000)
        + " "
        + ((double) record.getAllFee() / record.getAllTxCount() / 1000000)
        + " "
        + record.getChargeRecord().getTxCount()
        + " "
        + record.getChargeRecord().getEnergyCost()
        + " "
        + record.getChargeRecord().getBurnEnergy()
        + " "
        + ((double) record.getChargeRecord().getBurnEnergy()
            / record.getChargeRecord().getEnergyCost())
        + " "
        + ((double) record.getChargeRecord().getFee() / 1000000)
        + " "
        + ((double) record.getChargeRecord().getFee()
            / record.getChargeRecord().getTxCount()
            / 1000000)
        + " "
        + record.getWithdrawRecord().getTxCount()
        + " "
        + record.getWithdrawRecord().getEnergyCost()
        + " "
        + record.getWithdrawRecord().getBurnEnergy()
        + " "
        + ((double) record.getWithdrawRecord().getBurnEnergy()
            / record.getWithdrawRecord().getEnergyCost())
        + " "
        + ((double) record.getWithdrawRecord().getFee() / 1000000)
        + " "
        + ((double) record.getWithdrawRecord().getFee()
            / record.getWithdrawRecord().getTxCount()
            / 1000000)
        + " "
        + record.getCollectRecord().getTxCount()
        + " "
        + record.getCollectRecord().getEnergyCost()
        + " "
        + record.getCollectRecord().getBurnEnergy()
        + " "
        + ((double) record.getCollectRecord().getBurnEnergy()
            / record.getCollectRecord().getEnergyCost())
        + " "
        + ((double) record.getCollectRecord().getFee() / 1000000)
        + " "
        + ((double) record.getCollectRecord().getFee()
            / record.getCollectRecord().getTxCount()
            / 1000000);
  }

  private static String getSingleRecordString(SingleEnergyRecord record) {
    return record.getTxCount()
        + " "
        + record.getEnergyCost()
        + " "
        + record.getBurnEnergy()
        + " "
        + ((double) record.getBurnEnergy() / record.getEnergyCost())
        + " "
        + ((double) record.getFee() / 1000000)
        + " "
        + ((double) record.getFee() / record.getTxCount() / 1000000);
  }

  private static void tmpSync() throws BadItemException {

    DBIterator retIterator =
        (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
    retIterator.seek(ByteArray.fromLong(65366347));
    DBIterator blockIterator =
        (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
    blockIterator.seek(ByteArray.fromLong(65366347));

    EnergyRecord record = new EnergyRecord();
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
      if (blockNum > 65395138) {
        break;
      }

      byte[] value = retEntry.getValue();
      TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
      BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
      Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
      for (TransactionCapsule tx : blockCapsule.getTransactions()) {
        txCallerMap.put(tx.getTransactionId().toString(), tx);
      }

      for (Protocol.TransactionInfo transactionInfo :
          transactionRetCapsule.getInstance().getTransactioninfoList()) {
        byte[] txId = transactionInfo.getId().toByteArray();
        // toodo remove
        String txHash = Hex.toHexString(txId);
        TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));

        if (tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.TriggerSmartContract.class)
            || tx.getInstance()
                .getRawData()
                .getContract(0)
                .getParameter()
                .is(SmartContractOuterClass.CreateSmartContract.class)) {
          byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
          long fee = transactionInfo.getReceipt().getEnergyFee();
          long energyCost = transactionInfo.getReceipt().getEnergyUsageTotal();
          long burnEnergy =
              transactionInfo.getReceipt().getEnergyUsageTotal()
                  - transactionInfo.getReceipt().getEnergyUsage()
                  - transactionInfo.getReceipt().getOriginEnergyUsage();
          // 全网

          if (Arrays.equals(contractAddress, USDT_ADDR)) {
            record.addMainnetRecord(energyCost, burnEnergy, fee);
          }
        }
      }
    }
    System.out.println(
        record.mainnetRecord.getTxCount()
            + " "
            + record.mainnetRecord.getEnergyCost()
            + " "
            + record.mainnetRecord.burnEnergy
            + " "
            + record.mainnetRecord.fee);

    logger.info(
        record.mainnetRecord.getTxCount()
            + " "
            + record.mainnetRecord.getEnergyCost()
            + " "
            + record.mainnetRecord.burnEnergy
            + " "
            + record.mainnetRecord.fee);
  }
}
