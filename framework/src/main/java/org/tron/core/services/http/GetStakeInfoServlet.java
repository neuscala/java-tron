package org.tron.core.services.http;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.store.DynamicPropertiesStore;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
@Slf4j(topic = "API")
public class GetStakeInfoServlet extends RateLimiterServlet {

  @Autowired
  private DynamicPropertiesStore dps;

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      long num = dps.getCurrentCycleNumber();
      if (request.getParameter("num") != null) {
        num = Long.parseLong(request.getParameter("num"));
      }
      response.getWriter().println("{\"netStaked\": " + dps.getTotalNetWeight(num) +
              ",\"energyStaked\": " + dps.getTotalEnergyWeight(num) +
              ",\"netStaked2\": " + dps.getTotalNetWeight2(num) +
              ",\"energyStaked2\": " + dps.getTotalEnergyWeight2(num) + "}");
    } catch (Exception e) {
      logger.error("", e);
      try {
        response.getWriter().println(Util.printErrorMsg(e));
      } catch (IOException ioe) {
        logger.debug("IOException: {}", ioe.getMessage());
      }
    }
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    doGet(request, response);
  }
}
