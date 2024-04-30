package org.tron.core.services.http;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Objects;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.utils.Commons;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.store.AccountStore;
import org.tron.core.store.ContractStateStore;
import org.tron.protos.Protocol;

@Component
@Slf4j(topic = "API")
public class QueryAccountServlet extends RateLimiterServlet {

  @Autowired ContractStateStore contractStateStore;

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    try {
      Long date =
          request.getParameter("date") == null
              ? null
              : Long.parseLong(request.getParameter("date"));
      if (Objects.isNull(date)) {
        date =
            Long.valueOf(
                ContractStateStore.DATE_FORMAT.format(
                    System.currentTimeMillis() - 1000 * 60 * 60 * 24));
      }

      String address =
          request.getParameter("address") == null ? null : request.getParameter("address");
      if (StringUtils.isEmpty(address)) {
        address = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
        ContractStateCapsule cap = contractStateStore.getUsdtRecord(date);

        response.getWriter().println("{" + address + ": " + cap.toString() + "}");
      } else {

        Boolean isContract =
            request.getParameter("contract") == null
                ? null
                : Boolean.valueOf(request.getParameter("contract"));
        if (Objects.isNull(isContract)) {
          isContract = false;
        }
        byte[] addr = Commons.decodeFromBase58Check(address);

        if (Objects.nonNull(addr)) {
          ContractStateCapsule cap =
              isContract
                  ? contractStateStore.getContractRecord(date, addr)
                  : contractStateStore.getAccountRecord(date, addr);

          response.getWriter().println("{" + address + ": " + cap.toString() + "}");
        } else {
          response
              .getWriter()
              .println(
                  "Parsed addr is null: date "
                      + date
                      + ", address: "
                      + address
                      + ", contract: "
                      + isContract);
        }
      }
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
}
