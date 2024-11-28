package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;
import java.io.File;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.AllArgsConstructor;
import lombok.Data;
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
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.BlockCapsule;
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
import org.tron.core.store.AccountStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.protos.Protocol;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.Common;
import org.tron.protos.contract.SmartContractOuterClass;

import static org.tron.protos.Protocol.Transaction.Contract.ContractType.CancelAllUnfreezeV2Contract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.CreateSmartContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.FreezeBalanceContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.FreezeBalanceV2Contract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.TriggerSmartContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.UnfreezeBalanceContract;
import static org.tron.protos.Protocol.Transaction.Contract.ContractType.UnfreezeBalanceV2Contract;
import static org.tron.protos.Protocol.TransactionInfo.code.SUCESS;
import static org.tron.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.tron.protos.contract.Common.ResourceCode.ENERGY;

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
    //         appT.startup();
    //        appT.blockUntilShutdown();

    try {
      StakeInfo stakeInfo = initStakeInfo();

      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      // sync day stat
      long startTimestamp = 1682899200000L;
      long endTimestamp = 1732752000000L;
      long endBlockLastDay =
          ChainBaseManager.getInstance().getDynamicPropertiesStore().getLatestBlockHeaderNumber();
      long timeSpan = 1000 * 60 * 60 * 24;
      // todo remove test
//      endTimestamp =
//          ChainBaseManager.getInstance().getDynamicPropertiesStore().getLatestBlockHeaderTimestamp()
//              - timeSpan;
      for (long timestmap = endTimestamp; timestmap >= startTimestamp; timestmap -= timeSpan) {
        long curStartBlock = getBlockByTimestamp(timestmap) + 1;
        long curEndBlock = endBlockLastDay;
        endBlockLastDay = curStartBlock - 1;

        String date = dateFormat.format(timestmap);
        syncMevStat(curStartBlock, curEndBlock, dateFormat.format(timestmap), stakeInfo);
        String msg =
            date
                + " "
                + stakeInfo.v1energy
                + " "
                + stakeInfo.v1bandwidth
                + " "
                + stakeInfo.v2energy
                + " "
                + stakeInfo.v2bandwidth;
        logger.info(msg);
        System.out.println(msg);
      }
    } catch (Exception e) {
      logger.info("Sync Error!!!!", e);
    }

    logger.info("Sync End!!!!");
  }

  private static StakeInfo initStakeInfo() {
    // Init stake2.0
    AccountStore accountStore = ChainBaseManager.getInstance().getAccountStore();
    DynamicPropertiesStore dynamicPropertiesStore =
        ChainBaseManager.getInstance().getDynamicPropertiesStore();
    logger.info("Init stake info ... ");
    final AtomicLong count = new AtomicLong(0);
    DBIterator accountIterator = (DBIterator) accountStore.getDb().iterator();
    StakeInfo stakeInfo = new StakeInfo();
    while (accountIterator.hasNext()) {
      Map.Entry<byte[], byte[]> entry = accountIterator.next();
      byte[] value = entry.getValue();
      AccountCapsule accountCapsule = new AccountCapsule(value);

      // v1
      long v1bandwidth =
          accountCapsule.getFrozenBalance()
              + accountCapsule.getDelegatedFrozenBalanceForBandwidth();
      long v1energy =
          accountCapsule.getEnergyFrozenBalance()
              + accountCapsule.getDelegatedFrozenBalanceForEnergy();
      if (v1bandwidth > 0) {
        stakeInfo.addV1Bandwidth(v1bandwidth);
      }
      if (v1energy > 0) {
        stakeInfo.addV1Energy(v1energy);
      }

      // v2
      long v2bandwidth =
          accountCapsule.getFrozenV2BalanceForBandwidth()
              + accountCapsule.getDelegatedFrozenV2BalanceForBandwidth();
      long v2energy =
          accountCapsule.getFrozenV2BalanceForEnergy()
              + accountCapsule.getDelegatedFrozenV2BalanceForEnergy();
      if (v2bandwidth > 0) {
        stakeInfo.addV2Bandwidth(v2bandwidth);
      }
      if (v2energy > 0) {
        stakeInfo.addV2Energy(v2energy);
      }
      if (count.incrementAndGet() % 1_000_000 == 0) {
        logger.info("Init stake info, processed " + count.get());
        // todo remove test
//        if (count.get() >= 5_000_000) {
//          break;
//        }
      }
    }
    logger.info(
        "Init stake info end, v1 energy {} bandwidth {}, v2 energy {} bandwidth {}",
        stakeInfo.getV1energy(),
        stakeInfo.getV1bandwidth(),
        stakeInfo.getV2energy(),
        stakeInfo.getV2bandwidth());
    logger.info(
        "Total energy weight {}, bandwidth weight {}",
        dynamicPropertiesStore.getTotalEnergyWeight(),
        dynamicPropertiesStore.getTotalNetWeight());
    return stakeInfo;
  }

  private static void syncMevStat(long startBlock, long endBlock, String date, StakeInfo stakeInfo)
      throws BadItemException, InvalidProtocolBufferException {

    logger.info("Syncing date {} from {} to {}", date, startBlock, endBlock);
    if (startBlock > endBlock) {
      logger.info(
          "Syncing date {} from {} to {} end, start is bigger than end",
          date,
          startBlock,
          endBlock);
      return;
    }

    long newStakeV1Energy = 0;
    long newStakeV1Bandwidth = 0;
    long newStakeV2Energy = 0;
    long newStakeV2Bandwidth = 0;

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

      Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
      for (TransactionCapsule tx : blockCapsule.getTransactions()) {
        txCallerMap.put(tx.getTransactionId().toString(), tx);
      }

      for (Protocol.TransactionInfo transactionInfo :
          transactionRetCapsule.getInstance().getTransactioninfoList()) {
        byte[] txId = transactionInfo.getId().toByteArray();
        String txHash = Hex.toHexString(txId);
        TransactionCapsule tx = txCallerMap.get(txHash);

        try {
          if (transactionInfo.getResult().equals(SUCESS)) {
            if (tx.getInstance().getRawData().getContract(0).getType().equals(CreateSmartContract)
                || tx.getInstance()
                    .getRawData()
                    .getContract(0)
                    .getType()
                    .equals(TriggerSmartContract)) {
              for (Protocol.InternalTransaction it :
                  transactionInfo.getInternalTransactionsList()) {
                String note = new String(it.getNote().toByteArray());
                if (note.contains("freezeBalanceV2For")) {
                  long balance = it.getCallValueInfo(0).getCallValue();
                  if (note.contains("Energy")) {
                    newStakeV2Energy += balance;
                  } else if (note.contains("Bandwidth")) {
                    newStakeV2Bandwidth += balance;
                  }

                } else if (note.contains("unfreezeBalanceV2For")) {
                  long balance = it.getCallValueInfo(0).getCallValue();
                  if (note.contains("Energy")) {
                    newStakeV2Energy -= balance;
                  } else if (note.contains("Bandwidth")) {
                    newStakeV2Bandwidth -= balance;
                  }

                } else if (note.equals("cancelAllUnfreezeV2")) {
                  System.out.println(
                      "cancelAllUnfreezeV2 "
                          + txHash
                          + " "
                          + it.getHash()
                          + " "
                          + it.getCallerAddress());
                }
              }
            } else {
              if (tx.getInstance()
                  .getRawData()
                  .getContract(0)
                  .getType()
                  .equals(FreezeBalanceContract)) {
                BalanceContract.FreezeBalanceContract contract =
                    tx.getInstance()
                        .getRawData()
                        .getContract(0)
                        .getParameter()
                        .unpack(BalanceContract.FreezeBalanceContract.class);
                long balance = contract.getFrozenBalance();
                if (contract.getResource().equals(Common.ResourceCode.BANDWIDTH)) {
                  newStakeV1Bandwidth += balance;
                } else if (contract.getResource().equals(Common.ResourceCode.ENERGY)) {
                  newStakeV1Energy += balance;
                }

              } else if (tx.getInstance()
                  .getRawData()
                  .getContract(0)
                  .getType()
                  .equals(UnfreezeBalanceContract)) {
                BalanceContract.UnfreezeBalanceContract contract =
                    tx.getInstance()
                        .getRawData()
                        .getContract(0)
                        .getParameter()
                        .unpack(BalanceContract.UnfreezeBalanceContract.class);
                long balance = transactionInfo.getUnfreezeAmount();
                if (contract.getResource().equals(Common.ResourceCode.BANDWIDTH)) {
                  newStakeV1Bandwidth -= balance;
                } else if (contract.getResource().equals(Common.ResourceCode.ENERGY)) {
                  newStakeV1Energy -= balance;
                }

              } else if (tx.getInstance()
                  .getRawData()
                  .getContract(0)
                  .getType()
                  .equals(FreezeBalanceV2Contract)) {
                BalanceContract.FreezeBalanceV2Contract contract =
                    tx.getInstance()
                        .getRawData()
                        .getContract(0)
                        .getParameter()
                        .unpack(BalanceContract.FreezeBalanceV2Contract.class);
                long balance = contract.getFrozenBalance();
                if (contract.getResource().equals(Common.ResourceCode.BANDWIDTH)) {
                  newStakeV2Bandwidth += balance;
                } else if (contract.getResource().equals(Common.ResourceCode.ENERGY)) {
                  newStakeV2Energy += balance;
                }

              } else if (tx.getInstance()
                  .getRawData()
                  .getContract(0)
                  .getType()
                  .equals(UnfreezeBalanceV2Contract)) {
                BalanceContract.UnfreezeBalanceV2Contract contract =
                    tx.getInstance()
                        .getRawData()
                        .getContract(0)
                        .getParameter()
                        .unpack(BalanceContract.UnfreezeBalanceV2Contract.class);
                long balance = contract.getUnfreezeBalance();
                if (contract.getResource().equals(Common.ResourceCode.BANDWIDTH)) {
                  newStakeV2Bandwidth -= balance;
                } else if (contract.getResource().equals(Common.ResourceCode.ENERGY)) {
                  newStakeV2Energy -= balance;
                }

              } else if (tx.getInstance()
                  .getRawData()
                  .getContract(0)
                  .getType()
                  .equals(CancelAllUnfreezeV2Contract)) {
                Map<String, Long> map = transactionInfo.getCancelUnfreezeV2AmountMap();
                long bandwidthBalance = map.getOrDefault(BANDWIDTH.name(), 0L);
                long energyBalance = map.getOrDefault(ENERGY.name(), 0L);
                newStakeV2Bandwidth += bandwidthBalance;
                newStakeV2Energy += energyBalance;
              }
            }
          }
        } catch (Exception e) {
          logger.warn("Error parsing tx {}", txHash, e);
          throw e;
        }
      }
    }

    // total reverse calculate
    stakeInfo.addV1Energy(-newStakeV1Energy);
    stakeInfo.addV1Bandwidth(-newStakeV1Bandwidth);
    stakeInfo.addV2Energy(-newStakeV2Energy);
    stakeInfo.addV2Bandwidth(-newStakeV2Bandwidth);
  }

  @AllArgsConstructor
  @Data
  private static class StakeInfo {
    BigInteger v1energy;
    BigInteger v1bandwidth;
    BigInteger v2energy;
    BigInteger v2bandwidth;

    void addV1Energy(long value) {
      v1energy = v1energy.add(BigInteger.valueOf(value));
    }

    void addV1Bandwidth(long value) {
      v1bandwidth = v1bandwidth.add(BigInteger.valueOf(value));
    }

    void addV2Energy(long value) {
      v2energy = v2energy.add(BigInteger.valueOf(value));
    }

    void addV2Bandwidth(long value) {
      v2bandwidth = v2bandwidth.add(BigInteger.valueOf(value));
    }

    StakeInfo() {
      v1energy = BigInteger.ZERO;
      v1bandwidth = BigInteger.ZERO;
      v2energy = BigInteger.ZERO;
      v2bandwidth = BigInteger.ZERO;
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
}
