package org.tron.core.services.http;

import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.ByteString;
import io.netty.util.internal.StringUtil;
import java.io.IOException;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.api.GrpcAPI.Return;
import org.tron.api.GrpcAPI.Return.response_code;
import org.tron.api.GrpcAPI.TransactionExtention;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Commons;
import org.tron.core.Wallet;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.HeaderNotFound;
import org.tron.core.exception.VMIllegalException;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract;


@Component
@Slf4j(topic = "API")
public class TriggerConstantContractServlet extends RateLimiterServlet {

  @Autowired
  private Wallet wallet;

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    TriggerSmartContract.Builder build = TriggerSmartContract.newBuilder();
    TransactionExtention.Builder trxExtBuilder = TransactionExtention.newBuilder();
    Return.Builder retBuilder = Return.newBuilder();
    boolean visible = false;
    try {
      String contract = request.getReader().lines()
          .collect(Collectors.joining(System.lineSeparator()));
      Util.checkBodySize(contract);
      visible = Util.getVisiblePost(contract);
      Util.validateParameter(contract);
      JsonFormat.merge(contract, build, visible);
      JSONObject jsonObject = JSONObject.parseObject(contract);

      boolean isFunctionSelectorSet =
          !StringUtil.isNullOrEmpty(jsonObject.getString(Util.FUNCTION_SELECTOR));
      if (isFunctionSelectorSet) {
        String selector = jsonObject.getString(Util.FUNCTION_SELECTOR);
        String parameter = jsonObject.getString(Util.FUNCTION_PARAMETER);
        String data = Util.parseMethod(selector, parameter);
        build.setData(ByteString.copyFrom(ByteArray.fromHexString(data)));
      }

      TransactionCapsule trxCap = wallet
          .createTransactionCapsule(build.build(), ContractType.TriggerSmartContract);

      Transaction trx = wallet
          .triggerConstantContract(build.build(),trxCap,
              trxExtBuilder,
              retBuilder);
      trx = Util.setTransactionPermissionId(jsonObject, trx);
      trx = Util.setTransactionExtraData(jsonObject, trx, visible);
      trxExtBuilder.setTransaction(trx);
      retBuilder.setResult(true).setCode(response_code.SUCCESS);
    } catch (ContractValidateException e) {
      retBuilder.setResult(false).setCode(response_code.CONTRACT_VALIDATE_ERROR)
          .setMessage(ByteString.copyFromUtf8(e.getMessage()));
    } catch (Exception e) {
      String errString = null;
      if (e.getMessage() != null) {
        errString = e.getMessage().replaceAll("[\"]", "\'");
      }
      retBuilder.setResult(false).setCode(response_code.OTHER_ERROR)
          .setMessage(ByteString.copyFromUtf8(e.getClass() + " : " + errString));
    }
    trxExtBuilder.setResult(retBuilder);
    response.getWriter().println(Util.printTransactionExtention(trxExtBuilder.build(), visible));
  }

  public long getUSDTBalance(String address) {
    TriggerSmartContract.Builder build = TriggerSmartContract.newBuilder();
    TransactionExtention.Builder trxExtBuilder = TransactionExtention.newBuilder();
    Return.Builder retBuilder = Return.newBuilder();

    try {
      byte[] contractAddress = Commons.decodeFromBase58Check("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t");
      byte[] ownerAddress = Commons.decodeFromBase58Check("TKGRE6oiU3rEzasue4MsB6sCXXSTx9BAe3");
      String methodStr = "balanceOf(address)";
      String argsStr = "\"" + address + "\"";
      boolean isHex = false;
      byte[] input = Hex.decode(AbiUtil.parseMethod(methodStr, argsStr, isHex));
      build.setOwnerAddress(ByteString.copyFrom(ownerAddress));
      build.setContractAddress(ByteString.copyFrom(contractAddress));

      build.setData(ByteString.copyFrom(input));
      build.setCallValue(0);
      TriggerSmartContract triggerContract = build.build();

      TransactionCapsule trxCap =
          wallet.createTransactionCapsule(triggerContract, ContractType.TriggerSmartContract);

      Transaction trx =
          wallet.triggerConstantContract(triggerContract, trxCap, trxExtBuilder, retBuilder);
      //      trx = Util.setTransactionPermissionId(jsonObject, trx);
      //      trx = Util.setTransactionExtraData(jsonObject, trx, visible);
      String hexBalance = ByteArray.toHexString(trxExtBuilder.getConstantResult(0).toByteArray());
      return Long.parseLong(hexBalance, 16);
      //      trxExtBuilder.setTransaction(trx);
      //      retBuilder.setResult(true).setCode(response_code.SUCCESS);
    } catch (Exception e) {
      System.out.println(e);
      throw new RuntimeException(e);
    }
  }
}
