package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.Metrics;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
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
import org.tron.core.store.ContractStateStore;
import org.tron.core.store.DynamicPropertiesStore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.atomic.AtomicLong;

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

  /**
   * Start the FullNode.
   */
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
    TronApplicationContext context =
        new TronApplicationContext(beanFactory);
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
    RpcApiServiceOnSolidity rpcApiServiceOnSolidity = context
        .getBean(RpcApiServiceOnSolidity.class);
    appT.addService(rpcApiServiceOnSolidity);
    HttpApiOnSolidityService httpApiOnSolidityService = context
        .getBean(HttpApiOnSolidityService.class);
    if (CommonParameter.getInstance().solidityNodeHttpEnable) {
      appT.addService(httpApiOnSolidityService);
    }

    // JSON-RPC on solidity
    if (CommonParameter.getInstance().jsonRpcHttpSolidityNodeEnable) {
      JsonRpcServiceOnSolidity jsonRpcServiceOnSolidity = context
          .getBean(JsonRpcServiceOnSolidity.class);
      appT.addService(jsonRpcServiceOnSolidity);
    }

    // PBFT API (HTTP and GRPC)
    RpcApiServiceOnPBFT rpcApiServiceOnPBFT = context
        .getBean(RpcApiServiceOnPBFT.class);
    appT.addService(rpcApiServiceOnPBFT);
    HttpApiOnPBFTService httpApiOnPBFTService = context
        .getBean(HttpApiOnPBFTService.class);
    appT.addService(httpApiOnPBFTService);

    // JSON-RPC on PBFT
    if (CommonParameter.getInstance().jsonRpcHttpPBFTNodeEnable) {
      JsonRpcServiceOnPBFT jsonRpcServiceOnPBFT = context.getBean(JsonRpcServiceOnPBFT.class);
      appT.addService(jsonRpcServiceOnPBFT);
    }

    // Init stake2.0
    AccountStore accountStore = appT.getChainBaseManager().getAccountStore();
    DynamicPropertiesStore dynamicPropertiesStore = appT.getDbManager().getDynamicPropertiesStore();
    try {
      dynamicPropertiesStore.getTotalNetWeight2();
    } catch (Exception ignored) {
      dynamicPropertiesStore.saveTotalNetWeight2(0);
      dynamicPropertiesStore.saveTotalEnergyWeight2(0);
      System.out.println("Init stake2.0, start");
      final AtomicLong count = new AtomicLong(0);
      accountStore.forEach(e -> {
        long bandwidth = e.getValue().getFrozenV2BalanceForBandwidth()
                + e.getValue().getDelegatedFrozenV2BalanceForBandwidth();
        long energy = e.getValue().getFrozenV2BalanceForEnergy()
                + e.getValue().getDelegatedFrozenV2BalanceForEnergy();
        if (bandwidth > 0) {
          dynamicPropertiesStore.addTotalNetWeight2(bandwidth / 1_000_000);
        }
        if (energy > 0) {
          dynamicPropertiesStore.addTotalEnergyWeight2(energy / 1_000_000);
        }
        if (count.incrementAndGet() % 1_000_000 == 0) {
          System.out.println("Init stake2.0, processed " + count.get());
        }
      });
      System.out.println("Init stake2.0, end");
      System.out.println("Stake for bandwidth: " + dynamicPropertiesStore.getTotalNetWeight2());
      System.out.println("Stake for energy: " + dynamicPropertiesStore.getTotalEnergyWeight2());
    }

    //    appT.startup();
    //    appT.blockUntilShutdown();

    try {
      ContractStateStore contractStateStore = ChainBaseManager.getInstance().getContractStateStore();
      File file = new File("tron_usdt_addr.txt");
      BufferedReader reader = new BufferedReader(new FileReader(file));
      long count = 0;
      String address;
      while ((address = reader.readLine()) != null) {
        byte[] key = Hex.decode(address);
        ContractStateCapsule cap = contractStateStore.getAccountRecord(key);
        if (cap == null) {
          cap = new ContractStateCapsule(0);
        }
        cap.setSentUsdt(true);
        contractStateStore.setAccountRecord(key, cap);
        count++;

        if (count % 500000 == 0) {
          logger.info("Init usdt owned address count {}", count);
        }
      }

      logger.info("Init usdt owned success, total count {}.", count);
    } catch (Exception e) {
      logger.error("Init usdt owned failed, {}", e.getMessage());
    }
  }
}
