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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.Metrics;
import org.tron.common.utils.Commons;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.services.RpcApiService;
import org.tron.core.services.http.FullNodeHttpApiService;
import org.tron.core.services.http.NetUtil;
import org.tron.core.services.interfaceJsonRpcOnPBFT.JsonRpcServiceOnPBFT;
import org.tron.core.services.interfaceJsonRpcOnSolidity.JsonRpcServiceOnSolidity;
import org.tron.core.services.interfaceOnPBFT.RpcApiServiceOnPBFT;
import org.tron.core.services.interfaceOnPBFT.http.PBFT.HttpApiOnPBFTService;
import org.tron.core.services.interfaceOnSolidity.RpcApiServiceOnSolidity;
import org.tron.core.services.interfaceOnSolidity.http.solidity.HttpApiOnSolidityService;
import org.tron.core.services.jsonrpc.FullNodeJsonRpcHttpService;
import org.tron.core.store.ContractStateStore;

@Slf4j(topic = "app")
public class FullNode {

  private static ContractStateStore contractStateStore;

  private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static long ONE_DAY = 1000*60*60*24;

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
  public static void main(String[] args)
      throws IOException, NoSuchFieldException, IllegalAccessException {
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
    appT.startup();
    //    appT.blockUntilShutdown();
    long startCycle = 5847;
//    long startCycle = 6159;
    long startTimestamp = 1698796800000L;
//    long startTimestamp = 1705536000000L;
//    long endCycle = 6574;
    long endCycle = 6158;
//    long endCycle = 5854;
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    contractStateStore = ChainBaseManager.getInstance().getContractStateStore();
    PrintWriter writer = new PrintWriter("statistics.json");
    logger.info("Start to sync data, from cycle {} to {}", startCycle, endCycle);
    Map<String, Set<String>> cexAddresses = getCexAddresses();
    writer.println("[");
    for (long dayCycle = startCycle; dayCycle <= endCycle - 3; dayCycle += 4) {
      String dateStr = DATE_FORMAT.format(startTimestamp + (dayCycle - startCycle) * ONE_DAY / 4);
      String dayJson = getCycleString(dayCycle, 4, cexAddresses, dateStr);
      if (dayCycle + 3 < endCycle) {
        dayJson = dayJson + ", ";
      }
      writer.println(dayJson);
      logger.info("Sync startCycle {} cycleCount {}, date {}, finished", dayCycle, 4, dateStr);
    }
    writer.println("]");
    writer.close();
    logger.info("End sync data, from cycle {} to {}", startCycle, endCycle);
  }

  private static String getCycleString(
      long startCycle, long cycleCount, Map<String, Set<String>> cexAddresses, String dateStr)
      throws NoSuchFieldException, IllegalAccessException, IOException {

    boolean parsed = true;

    StringBuilder res = new StringBuilder("{ \"date\": \"" + dateStr + "\",\"USDT\": ");
    // USDT
    ContractStateCapsule usdt = getMergedCap(null, true, startCycle, cycleCount);
    res.append(parsed ? usdt.getUsdtOutput() : usdt.toString()).append(",");

    // Cex
    for (Map.Entry<String, Set<String>> entry : cexAddresses.entrySet()) {

      res.append("\"").append(entry.getKey()).append("\": ");

      ContractStateCapsule cap = new ContractStateCapsule(0);
      for (String addr : entry.getValue()) {
        ContractStateCapsule curCap =
            getMergedCap(Commons.decodeFromBase58Check(addr), false, startCycle, cycleCount);
        cap.merge(curCap);
      }

      res.append(cap).append(",");
      res.append("\"").append(entry.getKey() + "Parsed").append("\": ");
      res.append(parsed ? cap.getTransferFromToOutput() : cap.toString()).append(",");
    }

    // sunswap v1
    List<String> v1TriggerAddresses = readAddresses("sunswapv1trigger.txt");
    res.append("\"sunswap-v1\": ");

    ContractStateCapsule sunswapv1 = new ContractStateCapsule(0);
    for (String addr : v1TriggerAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      sunswapv1.merge(curCap);
    }

    res.append(parsed ? sunswapv1.getTriggerOutput() : sunswapv1.toString()).append(",");

    // sunswap v2
    List<String> v2TriggerAddresses = readAddresses("sunswapv2trigger.txt");
    res.append("\"sunswap-v2\": ");

    ContractStateCapsule sunswapv2 = new ContractStateCapsule(0);
    for (String addr : v2TriggerAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      sunswapv2.merge(curCap);
    }

    res.append(parsed ? sunswapv2.getTriggerOutput() : sunswapv2.toString()).append(",");

    // sunswap v3
    List<String> v3TriggerAddresses = readAddresses("sunswapv3trigger.txt");
    res.append("\"sunswap-v3\": ");

    ContractStateCapsule sunswapv3 = new ContractStateCapsule(0);
    for (String addr : v3TriggerAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      sunswapv3.merge(curCap);
    }

    res.append(parsed ? sunswapv3.getTriggerOutput() : sunswapv3.toString()).append(",");

    // TEo
    List<String> TEoAddresses = Arrays.asList("TCYoYqWStLzjWfj5Fuv2SdwgrXqCsBMNxN");
    res.append("\"TCY\": ");

    ContractStateCapsule TEo = new ContractStateCapsule(0);
    for (String addr : TEoAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      TEo.merge(curCap);
    }

    res.append(parsed ? TEo.getTriggerOutput() : TEo.toString()).append(",");

    // TGM
    List<String> TGMAddresses = Arrays.asList("TEorZTZ5MHx8SrvsYs1R3Ds5WvY1pVoMSA");
    res.append("\"TEo\": ");

    ContractStateCapsule TGM = new ContractStateCapsule(0);
    for (String addr : TGMAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      TGM.merge(curCap);
    }

    res.append(parsed ? TGM.getTriggerOutput() : TGM.toString()).append(",");

    // TGM
    List<String> TVaAddresses = Arrays.asList("TVaV2BBs8tpthbp19QAy7ibmXLoYsomKDD");
    res.append("\"TVa\": ");

    ContractStateCapsule TVa = new ContractStateCapsule(0);
    for (String addr : TVaAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      TVa.merge(curCap);
    }

    res.append(parsed ? TVa.getTriggerOutput() : TVa.toString()).append(",");

    // TGM
    List<String> TLnAddresses = Arrays.asList("TLn4TiTjHzVxvDtz5hbovKUb8EGWSnuN9f");
    res.append("\"TLn\": ");

    ContractStateCapsule TLn = new ContractStateCapsule(0);
    for (String addr : TLnAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      TLn.merge(curCap);
    }

    res.append(parsed ? TLn.getTriggerOutput() : TLn.toString()).append(",");

    // TXi
    List<String> TXiAddresses = Arrays.asList("TBWUraJGDQsZ5aYsuuzWQnWfD49L3U88FU");
    res.append("\"TBW\": ");

    ContractStateCapsule TXi = new ContractStateCapsule(0);
    for (String addr : TXiAddresses) {
      ContractStateCapsule curCap =
          getMergedCap(Commons.decodeFromBase58Check(addr), true, startCycle, cycleCount);
      TXi.merge(curCap);
    }

    res.append(parsed ? TXi.getTriggerOutput() : TXi.toString());

    res.append("}");
    return res.toString();
  }

  private static ContractStateCapsule getMergedCap(
      byte[] key, boolean isContract, long startCycle, long cycleCount)
      throws IllegalAccessException, NoSuchFieldException {
    ContractStateCapsule cap = getCap(key, isContract, startCycle);

    for (long cycle = 1; cycle < cycleCount; cycle++) {
      ContractStateCapsule curCap = getCap(key, isContract, startCycle + cycle);

      cap.merge(curCap);
    }

    return cap;
  }

  private static ContractStateCapsule getCap(byte[] key, boolean isContract, long cycle) {
    ContractStateCapsule cap;
    if (key == null) {
      cap = contractStateStore.getUsdtRecord(cycle);
    } else {
      cap =
          isContract
              ? contractStateStore.getContractRecord(cycle, key)
              : contractStateStore.getAccountRecord(cycle, key);
    }
    if (cap == null) {
      cap = new ContractStateCapsule(0);
    }
    return cap;
  }

  private static Map<String, Set<String>> getCexAddresses() {
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
                String cexName;
                if (name.contains("Binance")) {
                  cexName = "Binance";
                } else if (name.contains("Okex")) {
                  cexName = "Okex";
                } else if (name.contains("bybit")) {
                  cexName = "bybit";
                } else if (name.contains("MXC")) {
                  cexName = "MXC";
                } else if (name.contains("bitget")) {
                  cexName = "Bitget";
                } else if (name.contains("Kraken")) {
                  cexName = "Kraken";
                } else if (name.contains("WhiteBIT")) {
                  cexName = "WhiteBIT";
                } else if (name.contains("HTX")) {
                  cexName = "HTX";
                } else {
                  cexName = "Others";
                }
                Set<String> addrs = res.getOrDefault(cexName, new HashSet<>());
                addrs.add(addr);
                res.put(cexName, addrs);
              });
      return res;
    } catch (Exception e) {
      logger.error("Stat task getTronCexAddresses error, {}", e.getMessage());
      return new HashMap<>();
    }
  }

  private static List<String> readAddresses(String fileName) throws IOException {
    File file = new File(fileName);
    if (!file.exists()) {
      return new ArrayList<>();
    }

    BufferedReader reader = new BufferedReader(new FileReader(file));
    Set<String> addrs = new HashSet<>();
    String address;
    while ((address = reader.readLine()) != null) {
      addrs.add(address);
    }
    return new ArrayList<>(addrs);
  }
}
