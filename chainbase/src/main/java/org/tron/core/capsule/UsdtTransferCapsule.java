package org.tron.core.capsule;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.crypto.ECKey;
import org.tron.common.utils.Sha256Hash;
import org.tron.protos.contract.SmartContractOuterClass.UsdtTransfer;

@Slf4j(topic = "capsule")
public class UsdtTransferCapsule implements ProtoCapsule<UsdtTransfer> {

  private UsdtTransfer usdtTransfer;

  private static final JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();

  public UsdtTransferCapsule(UsdtTransfer usdtTransfer) {
    this.usdtTransfer = usdtTransfer;
  }

  public UsdtTransferCapsule(byte[] data) {
    try {
      this.usdtTransfer = UsdtTransfer.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      // logger.debug(e.getMessage());
    }
  }

  public UsdtTransferCapsule() {
    this.usdtTransfer = UsdtTransfer.newBuilder().build();
  }

  public UsdtTransferCapsule(
      Sha256Hash txId,
      byte[] fromAddr,
      byte[] toAddr,
      byte[] ownerAddr,
      byte[] contractAddr,
      long amount,
      long energyUsage,
      long energyUsageTotal,
      long energyPenaltyTotal,
      long fee) {
    this.usdtTransfer =
        UsdtTransfer.newBuilder()
            .setTxId(ByteString.copyFrom(txId.getBytes()))
            .setFromAddr(ByteString.copyFrom(fromAddr))
            .setToAddr(ByteString.copyFrom(toAddr))
            .setOwnerAddr(ByteString.copyFrom(ownerAddr))
            .setContractAddr(ByteString.copyFrom(contractAddr))
            .setAmount(amount)
            .setEnergyUsage(energyUsage)
            .setEnergyUsageTotal(energyUsageTotal)
            .setEnergyPenaltyTotal(energyPenaltyTotal)
            .setTrxBurn(fee)
            .build();
  }

  public UsdtTransferCapsule(
      Sha256Hash txId,
      byte[] fromAddr,
      byte[] toAddr,
      byte[] ownerAddr,
      byte[] contractAddr,
      long amount,
      long energyUsageTotal,
      long fee) {
    this.usdtTransfer =
        UsdtTransfer.newBuilder()
            .setTxId(ByteString.copyFrom(txId.getBytes()))
            .setFromAddr(ByteString.copyFrom(fromAddr))
            .setToAddr(ByteString.copyFrom(toAddr))
            .setOwnerAddr(ByteString.copyFrom(ownerAddr))
            .setContractAddr(ByteString.copyFrom(contractAddr))
            .setAmount(amount)
            .setEnergyUsageTotal(energyUsageTotal)
            .setTrxBurn(fee)
            .build();
  }

  @Override
  public byte[] getData() {
    return this.usdtTransfer.toByteArray();
  }

  @Override
  public UsdtTransfer getInstance() {
    return this.usdtTransfer;
  }

  public long getEnergyUsage() {
    return this.usdtTransfer.getEnergyUsage();
  }

  public void setEnergyUsage(long value) {
    this.usdtTransfer = this.usdtTransfer.toBuilder().setEnergyUsage(value).build();
  }

  public void addEnergyUsage(long toAdd) {
    setEnergyUsage(getEnergyUsage() + toAdd);
  }

  public long getEnergyUsageTotal() {
    return this.usdtTransfer.getEnergyUsageTotal();
  }

  public void addEnergyUsageTotal(long toAdd) {
    this.usdtTransfer =
        this.usdtTransfer.toBuilder()
            .setEnergyUsageTotal(this.usdtTransfer.getEnergyUsageTotal() + toAdd)
            .build();
  }

  public long getEnergyPenaltyTotal() {
    return this.usdtTransfer.getEnergyPenaltyTotal();
  }

  public void addEnergyPenaltyTotal(long toAdd) {
    this.usdtTransfer =
        this.usdtTransfer.toBuilder()
            .setEnergyPenaltyTotal(this.usdtTransfer.getEnergyPenaltyTotal() + toAdd)
            .build();
  }

  public long getFee() {
    return this.usdtTransfer.getTrxBurn();
  }

  public void addFee(long toAdd) {
    this.usdtTransfer =
        this.usdtTransfer.toBuilder().setTrxBurn(this.usdtTransfer.getTrxBurn() + toAdd).build();
  }

  public long getAmount() {
    return this.usdtTransfer.getAmount();
  }

  public void addAmount(long toAdd) {
    this.usdtTransfer =
        this.usdtTransfer.toBuilder().setAmount(this.usdtTransfer.getAmount() + toAdd).build();
  }

  public ByteString getTxId() {
    return this.usdtTransfer.getTxId();
  }

  public static String getBase64FromByteString(ByteString sign) {
    byte[] r = sign.substring(0, 32).toByteArray();
    byte[] s = sign.substring(32, 64).toByteArray();
    byte v = sign.byteAt(64);
    if (v < 27) {
      v += 27; //revId -> v
    }
    ECKey.ECDSASignature signature = ECKey.ECDSASignature.fromComponents(r, s, v);
    return signature.toBase64();
  }

  public ByteString getFromAddr() {
    return this.usdtTransfer.getFromAddr();
  }

  public ByteString getToAddr() {
    return this.usdtTransfer.getToAddr();
  }

  public ByteString getOwnerAddr() {
    return this.usdtTransfer.getOwnerAddr();
  }

  public ByteString getContractAddr() {
    return this.usdtTransfer.getContractAddr();
  }

  public void merge(UsdtTransferCapsule other) {
    if (other == null) {
      return;
    }
    
    addEnergyUsage(other.getEnergyUsage());
    addEnergyUsageTotal(other.getEnergyUsageTotal());
    addEnergyPenaltyTotal(other.getEnergyPenaltyTotal());
    addFee(other.getFee());
    addAmount(other.getAmount());
  }

  @Override
  public String toString() {
    try {
      return printer.print(usdtTransfer);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
