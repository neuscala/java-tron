package org.tron.core.services.http;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.utils.Commons;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.store.ContractStateStore;
import org.tron.core.store.DynamicPropertiesStore;

@Component
@Slf4j(topic = "API")
public class QueryAccountServlet extends RateLimiterServlet {

  @Autowired ContractStateStore contractStateStore;

  @Autowired DynamicPropertiesStore dynamicPropertiesStore;

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    try {

      Long startCycle =
          request.getParameter("start_cycle") == null
              ? null
              : Long.parseLong(request.getParameter("start_cycle"));
      if (Objects.isNull(startCycle)) {
        response.getWriter().println("start_cycle is empty!");
        return;
      }

      boolean isCex =
          request.getParameter("cex") != null && Boolean.parseBoolean(request.getParameter("cex"));
      if (isCex) {
        long cycleCount =
            request.getParameter("cycle_count") == null
                ? 4
                : Long.parseLong(request.getParameter("cycle_count"));
        ContractStateCapsule binance = contractStateStore.getBinanceRecord(startCycle);
        if (binance == null) {
          binance = new ContractStateCapsule(0);
        }
        ContractStateCapsule okex = contractStateStore.getOkexRecord(startCycle);
        if (okex == null) {
          okex = new ContractStateCapsule(0);
        }
        ContractStateCapsule bybit = contractStateStore.getBybitRecord(startCycle);
        if (bybit == null) {
          bybit = new ContractStateCapsule(0);
        }
        for (long cycle = 1; cycle < cycleCount; cycle++) {
          binance.merge(contractStateStore.getBinanceRecord(startCycle + cycle));
          okex.merge(contractStateStore.getOkexRecord(startCycle + cycle));
          bybit.merge(contractStateStore.getBybitRecord(startCycle + cycle));
        }

        response
            .getWriter()
            .println(
                "{\"Binance\": {"
                    + "\"energy_total\": "
                    + binance.getEnergyUsageTotal()
                    + ", \"energy_burn\": "
                    + binance.getEnergyUsage()
                    + "},\"Okex\": {"
                    + "\"energy_total\": "
                    + okex.getEnergyUsageTotal()
                    + ", \"energy_burn\": "
                    + okex.getEnergyUsage()
                    + "},\"Bybit\": {"
                    + "\"energy_total\": "
                    + bybit.getEnergyUsageTotal()
                    + ", \"energy_burn\": "
                    + bybit.getEnergyUsage()
                    + "}}");
        return;
      }

      long cycleCount =
          request.getParameter("cycle_count") == null
              ? 28L
              : Long.parseLong(request.getParameter("cycle_count"));

      boolean parsed =
          request.getParameter("parsed") == null
              || Boolean.parseBoolean(request.getParameter("parsed"));

      String address = request.getParameter("address");

      StringBuilder res = new StringBuilder("{\"USDT\": ");
      // USDT
      ContractStateCapsule usdt = getMergedCap(null, true, startCycle, cycleCount);
      res.append(parsed ? usdt.getUsdtOutput() : usdt.toString()).append(",");

      if (StringUtils.isEmpty(address)) {
        // Cex
        Map<String, Set<String>> cexAddresses = getCexAddresses();
        for (Map.Entry<String, Set<String>> entry : cexAddresses.entrySet()) {

          res.append("\"").append(entry.getKey()).append("\": ");

          ContractStateCapsule cap = new ContractStateCapsule(0);
          for (String addr : entry.getValue()) {
            ContractStateCapsule curCap =
                getMergedCap(Commons.decodeFromBase58Check(addr), false, startCycle, cycleCount);
            cap.merge(curCap);
          }

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

        List<String> v1VaultAddresses = readAddresses("sunswapv1vault.txt");
        res.append("\"sunswap-v1-vault\": ");

        ContractStateCapsule sunswapv1vault = new ContractStateCapsule(0);
        for (String addr : v1VaultAddresses) {
          ContractStateCapsule curCap =
              getMergedCap(Commons.decodeFromBase58Check(addr), false, startCycle, cycleCount);
          sunswapv1vault.merge(curCap);
        }

        res.append(parsed ? sunswapv1vault.getTransferFromToOutput() : sunswapv1vault.toString())
            .append(",");

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

        List<String> v2VaultAddresses = readAddresses("sunswapv2vault.txt");
        res.append("\"sunswap-v2-vault\": ");

        ContractStateCapsule sunswapv2vault = new ContractStateCapsule(0);
        for (String addr : v2VaultAddresses) {
          ContractStateCapsule curCap =
              getMergedCap(Commons.decodeFromBase58Check(addr), false, startCycle, cycleCount);
          sunswapv2vault.merge(curCap);
        }

        res.append(parsed ? sunswapv2vault.getTransferFromToOutput() : sunswapv2vault.toString())
            .append(",");

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

        List<String> v3VaultAddresses = readAddresses("sunswapv3vault.txt");
        res.append("\"sunswap-v3-vault\": ");

        ContractStateCapsule sunswapv3vault = new ContractStateCapsule(0);
        for (String addr : v3VaultAddresses) {
          ContractStateCapsule curCap =
              getMergedCap(Commons.decodeFromBase58Check(addr), false, startCycle, cycleCount);
          sunswapv3vault.merge(curCap);
        }

        res.append(parsed ? sunswapv3vault.getTransferFromToOutput() : sunswapv3vault.toString());
      } else {
        // address param
        res.append("\"").append(address).append("\": ");

        ContractStateCapsule curCap =
            getMergedCap(Commons.decodeFromBase58Check(address), false, startCycle, cycleCount);

        res.append(parsed ? curCap.getTransferFromToOutput() : curCap.toString());
      }

      res.append("}");

      response.getWriter().println(res);
    } catch (Exception e) {
      response
          .getWriter()
          .println(
              "Error: date "
                  + request.getParameter("date")
                  + ", address: "
                  + request.getParameter("address")
                  + ", contract: "
                  + request.getParameter("contract")
                  + ", errorMsg: "
                  + e.getMessage());
    }
  }

  private ContractStateCapsule getMergedCap(
      byte[] key, boolean isContract, long startCycle, long cycleCount)
      throws IllegalAccessException, NoSuchFieldException {
    ContractStateCapsule cap = getCap(key, isContract, startCycle);

    for (long cycle = 1; cycle < cycleCount; cycle++) {
      ContractStateCapsule curCap = getCap(key, isContract, startCycle + cycle);

      cap.merge(curCap);
    }

    return cap;
  }

  private ContractStateCapsule getCap(byte[] key, boolean isContract, long cycle) {
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

  private Map<String, Set<String>> getCexAddresses() {
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
                } else if (name.contains("Bybit")) {
                  cexName = "Bybit";
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

  private List<String> readAddresses(String fileName) throws IOException {
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
