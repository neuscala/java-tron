package org.tron.core.services.http;

import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
public class GetMevStatServlet extends RateLimiterServlet {

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

      long cycleCount =
          request.getParameter("cycle_count") == null
              ? 4
              : Long.parseLong(request.getParameter("cycle_count"));

      boolean detail =
          request.getParameter("detail") != null
              && Boolean.parseBoolean(request.getParameter("detail"));
      ContractStateCapsule targetAddr = contractStateStore.getMevTPsRecord(startCycle);
      if (targetAddr == null) {
        targetAddr = new ContractStateCapsule(0);
      }
      for (long cycle = 1; cycle < cycleCount; cycle++) {
        targetAddr.merge(contractStateStore.getMevTPsRecord(startCycle + cycle));
      }

      if (detail) {
        response
            .getWriter()
            .println(
                "{\"TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7\": {"
                    + "\"success_count\": "
                    + targetAddr.getSuccessAttackCount()
                    + ", \"attack_target_count\": "
                    + targetAddr.getAttemptAttackCount()
                    + ", \"attack_count\": "
                    + targetAddr.getPairAttackCount()
                    + ", \"success_profit\": "
                    + targetAddr.getProfit()
                    + ", \"fail_count\": "
                    + targetAddr.getFailAttackCount()
                    + ", \"fail_loss\": "
                    + targetAddr.getLoss()
                    + ", \"fuzzy_success_count\": "
                    + targetAddr.getFuzzySuccessAttackCount()
                    + ", \"fuzzy_fail_count\": "
                    + targetAddr.getFuzzyFailAttackCount()
                    + ", \"remaining_token_value\": "
                    + targetAddr.getRemainingTokenValue()
                    + ", \"attack_fee\": "
                    + targetAddr.getAttackFee()
                    + ", \"all_fee\": "
                    + targetAddr.getAllFee()
                    + ", \"success_buy\": "
                    + targetAddr.getSuccessBuyCount()
                    + ", \"success_sell\": "
                    + targetAddr.getSuccessSellCount()
                    + ", \"fail_buy\": "
                    + targetAddr.getFailBuyCount()
                    + ", \"fail_sell\": "
                    + targetAddr.getFailSellCount()
                    + "}}");
      } else {
        //	地址	成功次数（连续块）	攻击目标次数	攻击次数	成功交易获利	成本(亏损+剩余Token+攻击手续费)	实际获利	成功买交易	成功卖交易	失败买交易	失败卖交易
        long cost =
            targetAddr.getLoss()
                + Math.max(0, targetAddr.getRemainingTokenValue())
                + targetAddr.getAttackFee();
        response
            .getWriter()
            .println(
                "{\"TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7\": {"
                    + "\"success_count\": "
                    + (targetAddr.getSuccessAttackCount() - targetAddr.getFuzzySuccessAttackCount())
                    + ", \"attack_target_count\": "
                    + targetAddr.getAttemptAttackCount()
                    + ", \"attack_count\": "
                    + targetAddr.getPairAttackCount()
                    + ", \"success_profit\": "
                    + targetAddr.getProfit()
                    + ", \"cost\": "
                    + cost
                    + ", \"actual_profit\": "
                    + (targetAddr.getProfit() - cost)
                    + ", \"success_buy\": "
                    + targetAddr.getSuccessBuyCount()
                    + ", \"success_sell\": "
                    + targetAddr.getSuccessSellCount()
                    + ", \"fail_buy\": "
                    + targetAddr.getFailBuyCount()
                    + ", \"fail_sell\": "
                    + targetAddr.getFailSellCount()
                    + "}}");
      }
    } catch (Exception e) {
      response
          .getWriter()
          .println(
              "Error: start_cycle "
                  + request.getParameter("start_cycle")
                  + ", cycle_count: "
                  + request.getParameter("cycle_count")
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
                if (name.contains("targetAddr")) {
                  cexName = "targetAddr";
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
