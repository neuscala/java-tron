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
    byte[] LaunchPadProxy = Hex.decode("41C22DD1b7Bc7574e94563C8282F64B065bC07b2fa");
    byte[] TOKEN_PURCHASE_TOPIC =
        Hex.decode("63abb62535c21a5d221cf9c15994097b8880cc986d82faf80f57382b998dbae5");
    byte[] TOKEN_SOLD_TOPIC =
        Hex.decode("9387a595ac4be9038bbb9751abad8baa3dcf219dd9e19abb81552bd521fe3546");
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
    byte[] SWAP_TOPIC =
        Hex.decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");
    String WTRX = "891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";

    try {
      logger.info("Start To Local Test!!!");

      BufferedReader reader = new BufferedReader(new FileReader("paddrs.txt"));
      Set<String> paddrs = new HashSet<>();
      String line;
      while ((line = reader.readLine()) != null) {
        paddrs.add(Hex.toHexString(Commons.decodeFromBase58Check(line)));
      }
      new BufferedReader(new FileReader("saddrs.txt"));
      Set<String> saddrs = new HashSet<>();
      while ((line = reader.readLine()) != null) {
        saddrs.add(Hex.toHexString(Commons.decodeFromBase58Check(line)));
      }
      //      long startBlock = 64493430;
      //      long endBlock = 65092826;
      long startBlock = latestBlock - 2000;
      long endBlock = latestBlock - 1;
      long logBlock = startBlock;
      long pSumTxCount = 0;
      long pSumBuyCount = 0;
      long sSumTxCount = 0;
      long sSumBuyCount = 0;
      Map<String, AddrFailProfit> pAddrBuyMap = new HashMap<>();
      Map<String, AddrFailProfit> sAddrBuyMap = new HashMap<>();

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
        System.out.println(blockNum + " " + Longs.fromByteArray(blockEntry.getKey()));
        if (blockNum > endBlock) {
          break;
        }

        byte[] value = retEntry.getValue();
        TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
        BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());

        long timestamp = transactionRetCapsule.getInstance().getBlockTimeStamp();
        Map<String, Map<String, BuyAndSellRecordV2>> swapThisBlockMap =
            getThisBlockMap(swapLastBlockBuyAndSellMap);
        Map<String, Map<String, BuyAndSellRecordV2>> pumpThisBlockMap =
            getThisBlockMap(pumpLastBlockBuyAndSellMap);

        Map<String, String> txCallerMap = new HashMap<>();
        for (TransactionCapsule tx : blockCapsule.getTransactions()) {
          txCallerMap.put(tx.getTransactionId().toString(), Hex.toHexString(tx.getOwnerAddress()));
        }
        for (Protocol.TransactionInfo transactionInfo :
            transactionRetCapsule.getInstance().getTransactioninfoList()) {
          byte[] txId = transactionInfo.getId().toByteArray();
          String caller = txCallerMap.get(Hex.toHexString(txId));

          byte[] contractAddress = transactionInfo.getContractAddress().toByteArray();
          if (Arrays.equals(contractAddress, SWAP_ROUTER)) {
            sSumTxCount++;

            //            if (!saddrs.contains(caller)) {
            //              continue;
            //            }
            for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
              if (!Arrays.equals(log.getTopics(0).toByteArray(), SWAP_TOPIC)) {
                continue;
              }
              String pair = Hex.toHexString(log.getAddress().toByteArray());
              Map<String, BuyAndSellRecordV2> tokenMap =
                  swapThisBlockMap.getOrDefault(caller, new HashMap<>());
              BuyAndSellRecordV2 recordV2 = tokenMap.getOrDefault(pair, new BuyAndSellRecordV2());
              String logData = Hex.toHexString(log.getData().toByteArray());
              BigInteger amount0In = new BigInteger(logData.substring(0, 64), 16);
              BigInteger amount1In = new BigInteger(logData.substring(64, 128), 16);
              BigInteger amount0Out = new BigInteger(logData.substring(128, 192), 16);
              BigInteger amount1Out = new BigInteger(logData.substring(192, 256), 16);

              String token = pairToTokenMap.get(pair);
              if (token == null) {
                continue;
              }
              boolean smaller = smallerToWtrx(token, WTRX);

              boolean isBuy =
                  ((smaller && amount0Out.compareTo(BigInteger.ZERO) > 0)
                      || (!smaller && amount1Out.compareTo(BigInteger.ZERO) > 0));

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
                sSumBuyCount++;
                AddrFailProfit failProfit = sAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                failProfit.addSell(trxAmount);
                sAddrBuyMap.put(caller, failProfit);
                recordV2.addBuy(tokenAmount, trxAmount);
              } else {

                AddrFailProfit record = sAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                if (recordV2.sumBuyAmountToCover().compareTo(BigDecimal.ZERO) > 0) {

                  BigDecimal sumBuyAmountToCover = recordV2.sumBuyAmountToCover();
                  if (tokenAmount.compareTo(sumBuyAmountToCover) >= 0) {
                    recordV2.addSell(
                        tokenAmount,
                        trxAmount,
                        trxAmount.subtract(recordV2.trxSellAmountToCover()));
                    // 获利了
                    record.removeSell(recordV2.trxSellAmountToCover());
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
                    recordV2.addSell(
                        sellAmountRecord,
                        trxGetAmount,
                        trxGetAmount.subtract(recordV2.trxSellAmountToCover()));

                    // 补齐一部分
                    record.removeSell(recordV2.trxSellAmountToCover());

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
                      recordV2.addSell(
                          remainingSellAmountRecord,
                          remainingTrxGetAmount,
                          remainingTrxGetAmount.subtract(recordV2.trxSellAmountToCover()));
                      // 再补一部分
                      record.removeSell(recordV2.trxSellAmountToCover());
                    }
                  }
                } else {
                  record.addGet(trxAmount);
                }
                sAddrBuyMap.put(caller, record);
              }
              tokenMap.put(token, recordV2);
              swapThisBlockMap.put(caller, tokenMap);
              break;
            }
          } else if (Arrays.equals(contractAddress, SUNPUMP_LAUNCH)) {
            pSumTxCount++;
            //
            //            if (!paddrs.contains(caller)) {
            //              continue;
            //            }
            for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
              String token = Hex.toHexString(log.getAddress().toByteArray());
              Map<String, BuyAndSellRecordV2> tokenMap =
                  pumpThisBlockMap.getOrDefault(caller, new HashMap<>());
              BuyAndSellRecordV2 recordV2 = tokenMap.getOrDefault(token, new BuyAndSellRecordV2());

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

              String dataStr = Hex.toHexString(log.getData().toByteArray());
              BigDecimal trxAmount =
                  new BigDecimal(new BigInteger(dataStr.substring(0, 64), 16))
                      .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              BigDecimal feeAmount =
                  new BigDecimal(new BigInteger(dataStr.substring(64, 128), 16))
                      .divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
              BigDecimal tokenAmount =
                  new BigDecimal(new BigInteger(dataStr.substring(128, 192), 16))
                      .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
              if (isBuy) {
                pSumBuyCount++;
                AddrFailProfit failProfit =
                    pAddrBuyMap.getOrDefault(
                        caller, new AddrFailProfit(BigDecimal.ZERO, BigDecimal.ZERO, 0L));
                failProfit.addSell(feeAmount.add(trxAmount));
                pAddrBuyMap.put(caller, failProfit);
                recordV2.addBuy(tokenAmount, feeAmount.add(trxAmount));
              } else {

                AddrFailProfit record = pAddrBuyMap.getOrDefault(caller, new AddrFailProfit());
                if (recordV2.sumBuyAmountToCover().compareTo(BigDecimal.ZERO) > 0) {

                  BigDecimal sumBuyAmountToCover = recordV2.sumBuyAmountToCover();
                  if (tokenAmount.compareTo(sumBuyAmountToCover) >= 0) {
                    recordV2.addSell(
                        tokenAmount,
                        trxAmount,
                        trxAmount.subtract(recordV2.trxSellAmountToCover()));
                    // 获利了
                    record.removeSell(recordV2.trxSellAmountToCover());
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
                    recordV2.addSell(
                        sellAmountRecord,
                        trxGetAmount,
                        trxGetAmount.subtract(recordV2.trxSellAmountToCover()));

                    // 补齐一部分
                    record.removeSell(recordV2.trxSellAmountToCover());

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
                      recordV2.addSell(
                          remainingSellAmountRecord,
                          remainingTrxGetAmount,
                          remainingTrxGetAmount.subtract(recordV2.trxSellAmountToCover()));

                      // 再补
                      record.removeSell(recordV2.trxSellAmountToCover());
                    }
                  }
                } else {
                  record.addGet(trxAmount);
                }

                pAddrBuyMap.put(caller, record);
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
              "Sync to block {} timestamp {}, sum p addr {}, s addr {}, p_sum_tx_count {}, s_sum_tx_count {}",
              blockNum,
              timestamp,
              pAddrBuyMap.keySet().size(),
              sAddrBuyMap.keySet().size(),
              pSumTxCount,
              sSumTxCount);
        }
      }

      PrintWriter pwriter = new PrintWriter("finalresult.txt");
      pwriter.println("p_address sum_buy failed_profit");
      pAddrBuyMap.forEach(
          (k, v) ->
              pwriter.println(k + " " + v.buyCount + " " + (v.sumSellTrx.subtract(v.sumGetTrx))));
      pwriter.println("s_address sum_buy failed_profit");
      sAddrBuyMap.forEach(
          (k, v) ->
              pwriter.println(k + " " + v.buyCount + " " + (v.sumSellTrx.subtract(v.sumGetTrx))));

      pwriter.close();
      logger.info(
          "Final syncing sum p addr {}, s addr {}, p_sum_tx_count {}, p_buy {}, s_sum_tx_count {}, s_buy {}",
          pAddrBuyMap.keySet().size(),
          sAddrBuyMap.keySet().size(),
          pSumTxCount,
          pSumBuyCount,
          sSumTxCount,
          sSumBuyCount);

    } catch (Exception e) {
      logger.info("ERROR!!!", e);
    }

    logger.info("END");
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

    long buyCount;

    private AddrFailProfit() {
      this.sumSellTrx = BigDecimal.ZERO;
      this.sumGetTrx = BigDecimal.ZERO;
      this.buyCount = 0L;
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
      Map<String, Map<String, BuyAndSellRecordV2>> lastBlockBuyAndSellMap) {
    Map<String, Map<String, BuyAndSellRecordV2>> map = new HashMap<>();

    lastBlockBuyAndSellMap.forEach(
        (caller, tokenMap) ->
            tokenMap.forEach(
                (token, record) -> {
                  BuyAndSellRecordV2 recordV2 =
                      new BuyAndSellRecordV2(
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

    private BuyAndSellRecordV2() {
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
    }

    private BuyAndSellRecordV2(
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
    }

    private void addBuy(BigDecimal tokenAmount, BigDecimal trxSellAmount) {
      tokenBuyAmountThisBlock = tokenBuyAmountThisBlock.add(tokenAmount);
      this.trxSellAmountThisBlock = this.trxSellAmountThisBlock.add(trxSellAmount);
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

  @AllArgsConstructor
  @Getter
  private static class AddrAllInfoRecord {

    private BigDecimal trxAmount;
    private Set<Long> blocks;
    private Set<String> tokens;
    private long successCount;

    private AddrAllInfoRecord() {
      trxAmount = new BigDecimal(0);
      blocks = new HashSet<>();
      tokens = new HashSet<>();
      successCount = 0;
    }

    private void addRecord(long blockNum, String token, BigDecimal trxAmount, long success) {
      this.trxAmount = this.trxAmount.add(trxAmount);
      blocks.add(blockNum);
      tokens.add(token);
      successCount += success;
    }
  }
}
