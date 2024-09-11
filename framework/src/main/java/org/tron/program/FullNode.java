package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.fastjson.JSONObject;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;
import com.google.common.primitives.Longs;
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
import org.tron.common.utils.Commons;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.StringUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.TransactionStore;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.net.P2pEventHandlerImpl;
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
    //    appT.startup();
    //    appT.blockUntilShutdown();

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

      //      long startBlock = latestBlock - 5000;
      //      long endBlock = latestBlock - 1;
      //      long recentBlock = latestBlock - 2000;
      long startBlock = 64184959;
      long recentBlock = 64689819;
      logger.info(
          "Start To Local Test at {}!!! paddr size {}, saddr size {}",
          startBlock,
          paddrs.size(),
          saddrs.size());
      long endBlock = 65092826;
      long logBlock = startBlock;
      long pSumTxCount = 0;
      long pSumBuyCount = 0;
      long sSumTxCount = 0;
      long sSumBuyCount = 0;
      long pSumTxCountrecent = 0;
      long pSumBuyCountrecent = 0;
      long sSumTxCountrecent = 0;
      long sSumBuyCountrecent = 0;
      Map<String, AddrFailProfit> pAddrBuyMap = new HashMap<>();
      Map<String, AddrFailProfit> pAddrBuyMapRecent = new HashMap<>();
      Map<String, AddrFailProfit> sAddrBuyMap = new HashMap<>();
      Map<String, AddrFailProfit> sAddrBuyMapRecent = new HashMap<>();

      Map<String, Map<String, BuyAndSellRecordV2>> pumpLastBlockBuyAndSellMap = new HashMap<>();
      Map<String, Map<String, BuyAndSellRecordV2>> swapLastBlockBuyAndSellMap = new HashMap<>();

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
        if (blockNum > endBlock) {
          break;
        }

        byte[] value = retEntry.getValue();
        TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
        BlockCapsule blockCapsule;
        if (blockNum != blockStoreNum) {
          logger.error("BlockNum not equal!! {} {}", blockNum, blockStoreNum);
          blockIterator.seek(ByteArray.fromLong(blockNum + 1));
          blockCapsule = ChainBaseManager.getChainBaseManager().getBlockByNum(blockNum);
        } else {
          blockCapsule = new BlockCapsule(blockEntry.getValue());
        }

        long timestamp = transactionRetCapsule.getInstance().getBlockTimeStamp();
        Map<String, Map<String, BuyAndSellRecordV2>> swapThisBlockMap =
            getThisBlockMap(blockNum, swapLastBlockBuyAndSellMap);
        Map<String, Map<String, BuyAndSellRecordV2>> pumpThisBlockMap =
            getThisBlockMap(blockNum, pumpLastBlockBuyAndSellMap);

        Map<String, String> txCallerMap = new HashMap<>();
        for (TransactionCapsule tx : blockCapsule.getTransactions()) {
          txCallerMap.put(tx.getTransactionId().toString(), Hex.toHexString(tx.getOwnerAddress()));
        }

        if (blockCapsule.getTransactions().size()
            != transactionRetCapsule.getInstance().getTransactioninfoList().size()) {
          logger.error(
              "Tx size not equal!! {} {}",
              blockCapsule.getTransactions().size(),
              transactionRetCapsule.getInstance().getTransactioninfoList().size());
        }
        for (Protocol.TransactionInfo transactionInfo :
            transactionRetCapsule.getInstance().getTransactioninfoList()) {
          byte[] txId = transactionInfo.getId().toByteArray();
          String caller = get41Addr(txCallerMap.get(Hex.toHexString(txId)));

          byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
          if (Arrays.equals(contractAddress, SWAP_ROUTER)) {
            sSumTxCount++;
            if (blockNum >= recentBlock) {
              sSumTxCountrecent++;
            }

            if (!saddrs.contains(caller)) {
              continue;
            }
            for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
              if (!Arrays.equals(log.getTopics(0).toByteArray(), SWAP_TOPIC)) {
                continue;
              }
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
              Map<String, BuyAndSellRecordV2> tokenMap =
                  swapThisBlockMap.getOrDefault(caller, new HashMap<>());
              BuyAndSellRecordV2 recordV2 =
                  tokenMap.getOrDefault(token, new BuyAndSellRecordV2(blockNum));
              boolean smaller = smallerToWtrx(token, WTRX);
              token = get41Addr(token);

              boolean isBuy =
                  ((smaller && amount0Out.compareTo(BigInteger.ZERO) > 0)
                      || (!smaller && amount1Out.compareTo(BigInteger.ZERO) > 0));

              if (isBuy) {
                sSumBuyCount++;
                if (blockNum >= recentBlock) {
                  sSumBuyCountrecent++;
                }
              }

              BigDecimal trxAmount;
              BigDecimal tokenAmount;
              if (isBuy) {
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

              if (isBuy) {
                AddrFailProfit failProfit = sAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                failProfit.addSell(trxAmount);
                sAddrBuyMap.put(caller, failProfit);
                if (blockNum >= recentBlock) {
                  AddrFailProfit failProfitrecent =
                      sAddrBuyMapRecent.getOrDefault(caller, new AddrFailProfit());
                  failProfitrecent.addSell(trxAmount);
                  sAddrBuyMapRecent.put(caller, failProfitrecent);
                }
                recordV2.addBuy(tokenAmount, trxAmount);
              } else {

                AddrFailProfit record = sAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                AddrFailProfit recordrecent =
                    sAddrBuyMapRecent.getOrDefault(caller, new AddrFailProfit());
                if (recordV2.sumBuyAmountToCover().compareTo(BigDecimal.ZERO) > 0) {

                  BigDecimal sumBuyAmountToCover = recordV2.sumBuyAmountToCover();
                  if (tokenAmount.compareTo(sumBuyAmountToCover) >= 0) {
                    BigDecimal actualTokenAmount = sumBuyAmountToCover;
                    BigDecimal actualTrxAmount =
                        sumBuyAmountToCover
                            .multiply(trxAmount)
                            .divide(tokenAmount, 6, RoundingMode.HALF_EVEN);
                    recordV2.addSell(
                        actualTokenAmount,
                        actualTrxAmount,
                        actualTrxAmount.subtract(recordV2.trxSellAmountToCover()));
                    // 获利了
                    if (actualTrxAmount.compareTo(recordV2.trxSellAmountToCover()) < 0) {
                      record.addFailTrx(recordV2.trxSellAmountToCover().subtract(actualTrxAmount));
                      record.removeSell(actualTrxAmount);
                      if (blockNum >= recentBlock) {
                        recordrecent.addFailTrx(
                            recordV2.trxSellAmountToCover().subtract(actualTrxAmount));
                        recordrecent.removeSell(actualTrxAmount);
                      }
                    }
                  } else {
                    // 先和本块比较
                    BigDecimal sellAmountRecord =
                        tokenAmount.compareTo(recordV2.buyAmountToCoverThisBlock()) > 0
                            ? recordV2.buyAmountToCoverThisBlock()
                            : tokenAmount;
                    BigDecimal trxGetAmount =
                        sellAmountRecord
                            .multiply(trxAmount)
                            .divide(tokenAmount, 6, RoundingMode.HALF_EVEN);
                    // 补齐一部分
                    if (recordV2.tokenBuyAmountThisBlock.compareTo(BigDecimal.ZERO) > 0) {
                      record.removeSell(
                          sellAmountRecord
                              .multiply(recordV2.trxSellAmountThisBlock)
                              .divide(recordV2.tokenBuyAmountThisBlock, 6, RoundingMode.HALF_EVEN));
                      if (blockNum >= recentBlock) {
                        recordrecent.removeSell(
                            sellAmountRecord
                                .multiply(recordV2.trxSellAmountThisBlock)
                                .divide(
                                    recordV2.tokenBuyAmountThisBlock, 6, RoundingMode.HALF_EVEN));
                      }
                    }

                    recordV2.addSell(
                        sellAmountRecord,
                        trxGetAmount,
                        trxGetAmount.subtract(recordV2.trxSellAmountToCover()));

                    BigDecimal remainingToCover = sumBuyAmountToCover.subtract(sellAmountRecord);
                    BigDecimal remainingTokenAmount = sellAmountRecord.subtract(tokenAmount);
                    if (remainingToCover.compareTo(BigDecimal.ZERO) > 0
                        && remainingTokenAmount.compareTo(BigDecimal.ZERO) > 0) {

                      BigDecimal remainingSellAmountRecord =
                          remainingTokenAmount.compareTo(remainingToCover) > 0
                              ? remainingToCover
                              : remainingTokenAmount;
                      BigDecimal remainingTrxGetAmount =
                          remainingSellAmountRecord
                              .multiply(trxAmount)
                              .divide(tokenAmount, 6, RoundingMode.HALF_EVEN);
                      // 补齐一部分
                      if (recordV2.tokenBuyAmountLastBlock.compareTo(BigDecimal.ZERO) > 0) {
                        record.removeSell(
                            remainingSellAmountRecord
                                .multiply(recordV2.trxSellAmountLastBlock)
                                .divide(
                                    recordV2.tokenBuyAmountLastBlock, 6, RoundingMode.HALF_EVEN));
                        if (blockNum >= recentBlock) {
                          recordrecent.removeSell(
                              remainingSellAmountRecord
                                  .multiply(recordV2.trxSellAmountLastBlock)
                                  .divide(
                                      recordV2.tokenBuyAmountLastBlock, 6, RoundingMode.HALF_EVEN));
                        }
                      }
                      recordV2.addSell(
                          remainingSellAmountRecord,
                          remainingTrxGetAmount,
                          remainingTrxGetAmount.subtract(recordV2.trxSellAmountToCover()));
                    }
                  }
                } else {
                  record.addGet(trxAmount);
                  if (blockNum >= recentBlock) {
                    recordrecent.addGet(trxAmount);
                  }
                }
                sAddrBuyMap.put(caller, record);
                if (blockNum >= recentBlock) {
                  sAddrBuyMapRecent.put(caller, recordrecent);
                }
              }
              tokenMap.put(token, recordV2);
              swapThisBlockMap.put(caller, tokenMap);
              break;
            }
          } else if (Arrays.equals(contractAddress, SUNPUMP_LAUNCH)) {
            pSumTxCount++;
            if (blockNum >= recentBlock) {
              pSumTxCountrecent++;
            }
            for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
              String token = get41Addr(Hex.toHexString(log.getAddress().toByteArray()));
              Map<String, BuyAndSellRecordV2> tokenMap =
                  pumpThisBlockMap.getOrDefault(caller, new HashMap<>());
              BuyAndSellRecordV2 recordV2 =
                  tokenMap.getOrDefault(token, new BuyAndSellRecordV2(blockNum));

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

              String dataStr = Hex.toHexString(log.getData().toByteArray());
              //              BigDecimal trxAmount =
              //                  new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
              //                      .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              //              BigDecimal feeAmount =
              //                  new BigDecimal(new BigInteger(dataStr.substring(64, 128), 16))
              //                      .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              BigDecimal trxAmount = BigDecimal.ZERO;
              if (!isBuy) {
                for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
                  if (Arrays.equals(log2.getTopics(0).toByteArray(), TRX_RECEIVED)) {
                    trxAmount =
                        trxAmount.add(
                            new BigDecimal(
                                    new BigInteger(
                                        Hex.toHexString(log2.getData().toByteArray()), 16))
                                .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN));
                  }
                }
              } else {
                trxAmount =
                    new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
                        .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              }

              BigDecimal tokenAmount =
                  new BigDecimal(new BigInteger(dataStr.substring(128, 192), 16))
                      .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
              if (isBuy) {
                pSumBuyCount++;
                AddrFailProfit failProfit = pAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                failProfit.addSell(trxAmount);
                pAddrBuyMap.put(caller, failProfit);
                if (blockNum >= recentBlock) {
                  AddrFailProfit recentfailProfit =
                      pAddrBuyMapRecent.getOrDefault(caller, new AddrFailProfit());
                  recentfailProfit.addSell(trxAmount);
                  pAddrBuyMapRecent.put(caller, recentfailProfit);
                }
                recordV2.addBuy(tokenAmount, trxAmount);
              } else {

                AddrFailProfit record = pAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                AddrFailProfit recentrecord =
                    pAddrBuyMapRecent.getOrDefault(caller, new AddrFailProfit());
                if (recordV2.sumBuyAmountToCover().compareTo(BigDecimal.ZERO) > 0) {

                  BigDecimal sumBuyAmountToCover = recordV2.sumBuyAmountToCover();
                  if (tokenAmount.compareTo(sumBuyAmountToCover) >= 0) {
                    BigDecimal actualTokenAmount = sumBuyAmountToCover;
                    BigDecimal actualTrxAmount =
                        sumBuyAmountToCover
                            .multiply(trxAmount)
                            .divide(tokenAmount, 6, RoundingMode.HALF_EVEN);
                    recordV2.addSell(
                        actualTokenAmount,
                        actualTrxAmount,
                        actualTrxAmount.subtract(recordV2.trxSellAmountToCover()));
                    // 获利了
                    if (actualTrxAmount.compareTo(recordV2.trxSellAmountToCover()) < 0) {
                      record.addFailTrx(recordV2.trxSellAmountToCover().subtract(actualTrxAmount));
                      record.removeSell(actualTrxAmount);
                      if (blockNum >= recentBlock) {
                        recentrecord.addFailTrx(
                            recordV2.trxSellAmountToCover().subtract(actualTrxAmount));
                        recentrecord.removeSell(actualTrxAmount);
                      }
                    }
                  } else {
                    // 先和本块比较
                    BigDecimal sellAmountRecord =
                        tokenAmount.compareTo(recordV2.buyAmountToCoverThisBlock()) > 0
                            ? recordV2.buyAmountToCoverThisBlock()
                            : tokenAmount;
                    BigDecimal trxGetAmount =
                        sellAmountRecord
                            .multiply(trxAmount)
                            .divide(tokenAmount, 6, RoundingMode.HALF_EVEN);
                    // 补齐一部分
                    if (recordV2.tokenBuyAmountThisBlock.compareTo(BigDecimal.ZERO) > 0) {
                      record.removeSell(
                          sellAmountRecord
                              .multiply(recordV2.trxSellAmountThisBlock)
                              .divide(recordV2.tokenBuyAmountThisBlock, 6, RoundingMode.HALF_EVEN));
                      if (blockNum >= recentBlock) {
                        recentrecord.removeSell(
                            sellAmountRecord
                                .multiply(recordV2.trxSellAmountThisBlock)
                                .divide(
                                    recordV2.tokenBuyAmountThisBlock, 6, RoundingMode.HALF_EVEN));
                      }
                    }
                    recordV2.addSell(
                        sellAmountRecord,
                        trxGetAmount,
                        trxGetAmount.subtract(recordV2.trxSellAmountToCover()));

                    if (blockNum >= recentBlock) {
                      recentrecord.removeSell(recordV2.trxSellAmountToCover());
                    }

                    BigDecimal remainingToCover = sumBuyAmountToCover.subtract(sellAmountRecord);
                    BigDecimal remainingTokenAmount = sellAmountRecord.subtract(tokenAmount);
                    if (remainingToCover.compareTo(BigDecimal.ZERO) > 0
                        && remainingTokenAmount.compareTo(BigDecimal.ZERO) > 0) {

                      BigDecimal remainingSellAmountRecord =
                          remainingTokenAmount.compareTo(remainingToCover) > 0
                              ? remainingToCover
                              : remainingTokenAmount;
                      BigDecimal remainingTrxGetAmount =
                          remainingSellAmountRecord
                              .multiply(trxAmount)
                              .divide(tokenAmount, 6, RoundingMode.HALF_EVEN);
                      // 补齐一部分
                      if (recordV2.tokenBuyAmountLastBlock.compareTo(BigDecimal.ZERO) > 0) {
                        record.removeSell(
                            remainingSellAmountRecord
                                .multiply(recordV2.trxSellAmountLastBlock)
                                .divide(
                                    recordV2.tokenBuyAmountLastBlock, 6, RoundingMode.HALF_EVEN));
                        if (blockNum >= recentBlock) {
                          recentrecord.removeSell(
                              remainingSellAmountRecord
                                  .multiply(recordV2.trxSellAmountLastBlock)
                                  .divide(
                                      recordV2.tokenBuyAmountLastBlock, 6, RoundingMode.HALF_EVEN));
                        }
                      }
                      recordV2.addSell(
                          remainingSellAmountRecord,
                          remainingTrxGetAmount,
                          remainingTrxGetAmount.subtract(recordV2.trxSellAmountToCover()));

                      if (blockNum >= recentBlock) {
                        recentrecord.removeSell(recordV2.trxSellAmountToCover());
                      }
                    }
                  }
                } else {
                  record.addGet(trxAmount);
                  if (blockNum >= recentBlock) {
                    recentrecord.addGet(trxAmount);
                  }
                }

                pAddrBuyMap.put(caller, record);
                if (blockNum >= recentBlock) {
                  pAddrBuyMapRecent.put(caller, recentrecord);
                }
              }
              tokenMap.put(token, recordV2);
              pumpThisBlockMap.put(caller, tokenMap);
              break;
            }
          }
        }
        swapLastBlockBuyAndSellMap = new HashMap<>(swapThisBlockMap);
        pumpLastBlockBuyAndSellMap = new HashMap<>(pumpThisBlockMap);

        if (blockNum - logBlock >= 10000) {
          logBlock = blockNum;
          logger.info(
              "Sync to block {} timestamp {}, sum p addr {}, recent p addr {}, p_sum_tx_count {}, s_sum_tx_count {}",
              blockNum,
              timestamp,
              pAddrBuyMap.keySet().size(),
              pAddrBuyMapRecent.keySet().size(),
              pSumTxCount,
              sSumTxCount);
        }
      }

      PrintWriter pwriter = new PrintWriter("finalresult.txt");
      pwriter.println("p_address sum_buy failed_profit");
      pAddrBuyMap.forEach(
          (k, v) ->
              pwriter.println(
                  StringUtil.encode58Check(Hex.decode(k))
                      + " "
                      + v.buyCount
                      + " "
                      + (v.sumSellTrx.subtract(v.sumGetTrx))
                      + " "
                      + v.failedTrx));
      pwriter.println("recentp_address sum_buy failed_profit");
      pAddrBuyMapRecent.forEach(
          (k, v) ->
              pwriter.println(
                  StringUtil.encode58Check(Hex.decode(k))
                      + " "
                      + v.buyCount
                      + " "
                      + (v.sumSellTrx.subtract(v.sumGetTrx))
                      + " "
                      + v.failedTrx));
      pwriter.println("s_address sum_buy failed_profit");
      sAddrBuyMap.forEach(
          (k, v) ->
              pwriter.println(
                  StringUtil.encode58Check(Hex.decode(k))
                      + " "
                      + v.buyCount
                      + " "
                      + (v.sumSellTrx.subtract(v.sumGetTrx))
                      + " "
                      + v.failedTrx));
      pwriter.println("recents_address sum_buy failed_profit");
      sAddrBuyMapRecent.forEach(
          (k, v) ->
              pwriter.println(
                  StringUtil.encode58Check(Hex.decode(k))
                      + " "
                      + v.buyCount
                      + " "
                      + (v.sumSellTrx.subtract(v.sumGetTrx))
                      + " "
                      + v.failedTrx));

      pwriter.close();
      logger.info(
          "Final syncing sum p addr {}, s addr {}, p_sum_tx_count {}, p_buy {}, s_sum_tx_count {}, s_buy {}, p_recent_tx_count {}, p_recent_buy {}, s_recent_tx_count {}, s_recent_buy {}",
          pAddrBuyMap.keySet().size(),
          sAddrBuyMap.keySet().size(),
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

  private static String get41Addr(String hexAddr) {
    if (!hexAddr.startsWith("41")) {
      return "41" + hexAddr;
    }
    return hexAddr;
  }

  private static Map<String, String> populateMap() throws IOException {
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

  @AllArgsConstructor
  private static class AddrFailProfit {
    BigDecimal sumSellTrx;

    BigDecimal sumGetTrx;
    BigDecimal failedTrx;

    long buyCount;

    private AddrFailProfit() {
      this.sumSellTrx = BigDecimal.ZERO;
      this.sumGetTrx = BigDecimal.ZERO;
      this.failedTrx = BigDecimal.ZERO;
      this.buyCount = 0L;
    }

    private void addFailTrx(BigDecimal amount) {
      this.failedTrx = this.failedTrx.add(amount);
    }

    private void addSell(BigDecimal amount) {
      this.sumSellTrx = this.sumSellTrx.add(amount);
      this.buyCount++;
    }

    private void addGet(BigDecimal amount) {
      this.sumGetTrx = this.sumGetTrx.add(amount);
      if (sumGetTrx.compareTo(sumSellTrx) > 0) {
        sumGetTrx = sumSellTrx.add(BigDecimal.ZERO);
      }
    }

    private void removeSell(BigDecimal amount) {
      this.sumSellTrx = this.sumSellTrx.subtract(amount);
      if (sumSellTrx.compareTo(sumGetTrx) < 0) {
        sumSellTrx = sumGetTrx.add(BigDecimal.ZERO);
      }
    }

    private void addBuyCount() {
      this.buyCount++;
    }
  }

  private static boolean smallerToWtrx(String token, String wtrx) {
    return compareMap.computeIfAbsent(token, t -> token.compareTo(wtrx) < 0);
  }

  private static Map<String, Map<String, BuyAndSellRecordV2>> getThisBlockMap(
      long curBlockNum, Map<String, Map<String, BuyAndSellRecordV2>> lastBlockBuyAndSellMap) {
    Map<String, Map<String, BuyAndSellRecordV2>> map = new HashMap<>();

    lastBlockBuyAndSellMap.forEach(
        (caller, tokenMap) ->
            tokenMap.forEach(
                (token, record) -> {
                  if (curBlockNum - record.blockNum != 1
                      || record
                              .getTokenBuyAmountThisBlock()
                              .subtract(record.getTokenSellAmountThisBlock())
                              .compareTo(BigDecimal.ZERO)
                          <= 0) {
                    return;
                  }
                  BuyAndSellRecordV2 recordV2 =
                      new BuyAndSellRecordV2(
                          curBlockNum,
                          record.tokenBuyAmountThisBlock,
                          record.trxSellAmountThisBlock,
                          record.tokenSellAmountThisBlock,
                          record.trxBuyAmountThisBlock);
                  Map<String, BuyAndSellRecordV2> thisBlockTokenMap =
                      map.getOrDefault(caller, new HashMap<>());
                  thisBlockTokenMap.put(token, recordV2);
                  map.put(caller, thisBlockTokenMap);
                }));
    return map;
  }

  @AllArgsConstructor
  @Getter
  private static class BuyAndSellRecordV2 {

    BigDecimal tokenBuyAmountThisBlock;
    BigDecimal trxSellAmountThisBlock;
    BigDecimal tokenSellAmountThisBlock;
    BigDecimal trxBuyAmountThisBlock;
    BigDecimal tokenBuyAmountLastBlock;
    BigDecimal trxSellAmountLastBlock;
    BigDecimal tokenSellAmountLastBlock;
    BigDecimal trxBuyAmountLastBlock;
    long successCount;
    BigDecimal recordProfit;

    long blockNum;

    private BuyAndSellRecordV2(long blockNum) {
      tokenBuyAmountThisBlock = BigDecimal.ZERO;
      trxSellAmountThisBlock = BigDecimal.ZERO;
      tokenSellAmountThisBlock = BigDecimal.ZERO;
      trxBuyAmountThisBlock = BigDecimal.ZERO;
      tokenBuyAmountLastBlock = BigDecimal.ZERO;
      trxSellAmountLastBlock = BigDecimal.ZERO;
      tokenSellAmountLastBlock = BigDecimal.ZERO;
      trxBuyAmountLastBlock = BigDecimal.ZERO;
      successCount = 0;
      recordProfit = BigDecimal.ZERO;
      this.blockNum = blockNum;
    }

    private BuyAndSellRecordV2(
        long blockNum,
        BigDecimal tokenBuyAmountLastBlock,
        BigDecimal trxSellAmountLastBlock,
        BigDecimal tokenSellAmountLastBlock,
        BigDecimal trxBuyAmountLastBlock) {
      tokenBuyAmountThisBlock = BigDecimal.ZERO;
      trxSellAmountThisBlock = BigDecimal.ZERO;
      tokenSellAmountThisBlock = BigDecimal.ZERO;
      trxBuyAmountThisBlock = BigDecimal.ZERO;
      this.tokenBuyAmountLastBlock = tokenBuyAmountLastBlock;
      this.trxSellAmountLastBlock = trxSellAmountLastBlock;
      this.tokenSellAmountLastBlock = tokenSellAmountLastBlock;
      this.trxBuyAmountLastBlock = trxBuyAmountLastBlock;
      successCount = 0;
      recordProfit = BigDecimal.ZERO;
      this.blockNum = blockNum;
    }

    private void addBuy(BigDecimal tokenAmount, BigDecimal trxSellAmount) {
      tokenBuyAmountThisBlock = tokenBuyAmountThisBlock.add(tokenAmount);
      this.trxSellAmountThisBlock = this.trxSellAmountThisBlock.add(trxSellAmount);
    }

    private BigDecimal getTrxByToken(BigDecimal tokenAmount) {
      return (this.trxSellAmountThisBlock.add(this.trxSellAmountLastBlock))
          .multiply(tokenAmount)
          .divide(
              (this.tokenBuyAmountThisBlock.add(this.tokenBuyAmountLastBlock)),
              6,
              RoundingMode.HALF_EVEN);
    }

    private void addSell(BigDecimal tokenAmount, BigDecimal trxBuyAmount, BigDecimal profit) {
      tokenSellAmountThisBlock = tokenSellAmountThisBlock.add(tokenAmount);
      this.trxBuyAmountThisBlock = this.trxBuyAmountThisBlock.add(trxBuyAmount);

      if (profit.compareTo(BigDecimal.ZERO) > 0) {
        successCount++;
        recordProfit = recordProfit.add(profit);
      }
    }

    private boolean hasBuyThisBlock() {
      return tokenBuyAmountThisBlock.compareTo(BigDecimal.ZERO) > 0;
    }

    private boolean hasSellThisBlock() {
      return tokenSellAmountThisBlock.compareTo(BigDecimal.ZERO) > 0;
    }

    private BigDecimal buyAmountToCoverThisBlock() {
      return tokenBuyAmountThisBlock.subtract(tokenSellAmountThisBlock);
    }

    private BigDecimal buyAmountToCoverLastBlock() {
      return tokenBuyAmountLastBlock.subtract(tokenSellAmountLastBlock);
    }

    private BigDecimal sumBuyAmountToCover() {
      return tokenBuyAmountThisBlock
          .add(tokenBuyAmountLastBlock)
          .subtract(tokenSellAmountThisBlock)
          .subtract(tokenSellAmountLastBlock);
    }

    private BigDecimal mevTrxProfitAmount() {
      return trxBuyAmountThisBlock
          .add(trxBuyAmountLastBlock)
          .subtract(trxSellAmountThisBlock)
          .subtract(trxSellAmountLastBlock);
    }

    private BigDecimal trxSellAmountToCover() {
      return trxSellAmountThisBlock
          .add(trxSellAmountLastBlock)
          .subtract(trxBuyAmountThisBlock)
          .subtract(trxBuyAmountLastBlock);
    }
  }
}
