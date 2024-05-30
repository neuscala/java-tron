package org.tron.core.capsule;

import static org.tron.core.Constant.DYNAMIC_ENERGY_DECREASE_DIVISION;
import static org.tron.core.Constant.DYNAMIC_ENERGY_FACTOR_DECIMAL;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.protos.contract.SmartContractOuterClass;
import org.tron.protos.contract.SmartContractOuterClass.ContractState;

import java.math.BigInteger;

@Slf4j(topic = "capsule")
public class ContractStateCapsule implements ProtoCapsule<ContractState> {

  private static final BigInteger ONE_TRX = new BigInteger(String.valueOf(1_000_1000L));
  private static final BigInteger TEN_TRX = new BigInteger(String.valueOf(10_000_1000L));
  private static final BigInteger HUNDRED_TRX = new BigInteger(String.valueOf(100_000_1000L));
  private static final BigInteger THOUSAND_TRX = new BigInteger(String.valueOf(1_000_000_1000L));
  private static final BigInteger TEN_THOUSAND_TRX = new BigInteger(String.valueOf(10_000_000_1000L));

  private ContractState contractState;

  public ContractStateCapsule(ContractState contractState) {
    this.contractState = contractState;
  }

  public ContractStateCapsule(byte[] data) {
    try {
      this.contractState = SmartContractOuterClass.ContractState.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      // logger.debug(e.getMessage());
    }
  }

  public ContractStateCapsule(long currentCycle) {
    reset(currentCycle);
  }

  @Override
  public byte[] getData() {
    return this.contractState.toByteArray();
  }

  @Override
  public ContractState getInstance() {
    return this.contractState;
  }

  public long getEnergyUsage() {
    return this.contractState.getEnergyUsage();
  }

  public void setEnergyUsage(long value) {
    this.contractState = this.contractState.toBuilder().setEnergyUsage(value).build();
  }

  public void addEnergyUsage(long toAdd) {
    setEnergyUsage(getEnergyUsage() + toAdd);
  }

  public long getEnergyFactor() {
    return this.contractState.getEnergyFactor();
  }

  public void setEnergyFactor(long value) {
    this.contractState = this.contractState.toBuilder().setEnergyFactor(value).build();
  }

  public long getUpdateCycle() {
    return this.contractState.getUpdateCycle();
  }

  public void setUpdateCycle(long value) {
    this.contractState = this.contractState.toBuilder().setUpdateCycle(value).build();
  }

  public void addUpdateCycle(long toAdd) {
    setUpdateCycle(getUpdateCycle() + toAdd);
  }

  public long getEnergyUsageTotal() {
    return this.getInstance().getEnergyUsageTotal();
  }

  public void addEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setEnergyUsageTotal(this.contractState.getEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getEnergyUsageFailed() {
    return this.getInstance().getEnergyUsageFailed();
  }

  public void addEnergyUsageFailed(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setEnergyUsageFailed(this.contractState.getEnergyUsageFailed() + toAdd)
        .build();
  }

  public long getEnergyPenaltyTotal() {
    return this.getInstance().getEnergyPenaltyTotal();
  }

  public void addEnergyPenaltyTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setEnergyPenaltyTotal(this.contractState.getEnergyPenaltyTotal() + toAdd)
        .build();
  }

  public long getEnergyPenaltyFailed() {
    return this.getInstance().getEnergyPenaltyFailed();
  }

  public void addEnergyPenaltyFailed(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setEnergyPenaltyFailed(this.contractState.getEnergyPenaltyFailed() + toAdd)
        .build();
  }

  public long getTrxBurn() {
    return this.getInstance().getTrxBurn();
  }

  public void addTrxBurn(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTrxBurn(this.contractState.getTrxBurn() + toAdd)
        .build();
  }

  public long getTrxPenalty() {
    return this.getInstance().getTrxPenalty();
  }

  public void addTrxPenalty(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTrxPenalty(this.contractState.getTrxPenalty() + toAdd)
        .build();
  }

  public long getTxTotalCount() {
    return this.getInstance().getTxTotalCount();
  }

  public void addTxTotalCount() {
    this.contractState = this.contractState.toBuilder()
        .setTxTotalCount(this.contractState.getTxTotalCount() + 1)
        .build();
  }

  public void addTxTotalCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTxTotalCount(this.contractState.getTxTotalCount() + toAdd)
        .build();
  }

  public long getTxFailedCount() {
    return this.getInstance().getTxFailedCount();
  }

  public void addTxFailedCount() {
    this.contractState = this.contractState.toBuilder()
        .setTxFailedCount(this.contractState.getTxFailedCount() + 1)
        .build();
  }

  public void addTxFailedCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTxFailedCount(this.contractState.getTxFailedCount() + toAdd)
        .build();
  }

  public long getTransferCount() {
    return this.getInstance().getTransferCount();
  }

  public void addTransferCount() {
    this.contractState = this.contractState.toBuilder()
        .setTransferCount(this.contractState.getTransferCount() + 1)
        .build();
  }

  public void addTransferCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferCount(this.contractState.getTransferCount() + toAdd)
        .build();
  }

  public long getTransferFromCount() {
    return this.getInstance().getTransferFromCount();
  }

  public void addTransferFromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromCount(this.contractState.getTransferFromCount() + 1)
        .build();
  }

  public void addTransferFromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromCount(this.contractState.getTransferFromCount() + toAdd)
        .build();
  }

  public long getTransferFee() {
    return this.getInstance().getTransferFee();
  }

  public void addTransferFee() {
    this.contractState = this.contractState.toBuilder()
        .setTransferFee(this.contractState.getTransferFee() + 1)
        .build();
  }

  public void addTransferFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferFee(this.contractState.getTransferFee() + toAdd)
        .build();
  }

  public long getTransferFromFee() {
    return this.getInstance().getTransferFromFee();
  }

  public void addTransferFromFee() {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromFee(this.contractState.getTransferFromFee() + 1)
        .build();
  }

  public void addTransferFromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromFee(this.contractState.getTransferFromFee() + toAdd)
        .build();
  }

  public long getTransferEnergyUsage() {
    return this.getInstance().getTransferEnergyUsage();
  }

  public void addTransferEnergyUsage() {
    this.contractState = this.contractState.toBuilder()
        .setTransferEnergyUsage(this.contractState.getTransferEnergyUsage() + 1)
        .build();
  }

  public void addTransferEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferEnergyUsage(this.contractState.getTransferEnergyUsage() + toAdd)
        .build();
  }

  public long getTransferFromEnergyUsage() {
    return this.getInstance().getTransferFromEnergyUsage();
  }

  public void addTransferFromEnergyUsage() {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromEnergyUsage(this.contractState.getTransferFromEnergyUsage() + 1)
        .build();
  }

  public void addTransferFromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromEnergyUsage(this.contractState.getTransferFromEnergyUsage() + toAdd)
        .build();
  }


  public long getTransferEnergyPenalty() {
    return this.getInstance().getTransferEnergyPenalty();
  }

  public void addTransferEnergyPenalty(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferEnergyPenalty(this.contractState.getTransferEnergyPenalty() + toAdd)
        .build();
  }

  public long getTransferFromEnergyPenalty() {
    return this.getInstance().getTransferFromEnergyPenalty();
  }

  public void addTransferFromEnergyPenalty(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromEnergyPenalty(this.contractState.getTransferFromEnergyPenalty() + toAdd)
        .build();
  }

  public long getTransferNewEnergyUsage() {
    return this.getInstance().getTransferNewEnergyUsage();
  }

  public void addTransferNewEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferNewEnergyUsage(this.contractState.getTransferNewEnergyUsage() + toAdd)
        .build();
  }

  public long getTransferFromNewEnergyUsage() {
    return this.getInstance().getTransferFromNewEnergyUsage();
  }

  public void addTransferFromNewEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTransferFromNewEnergyUsage(this.contractState.getTransferFromNewEnergyUsage() + toAdd)
        .build();
  }

  public long getTsFromCount() {
    return this.getInstance().getTsFromCount();
  }

  public void addTsFromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsFromCount(this.contractState.getTsFromCount() + 1)
        .build();
  }

  public void addTsFromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsFromCount(this.contractState.getTsFromCount() + toAdd)
        .build();
  }

  public long getTsToCount() {
    return this.getInstance().getTsToCount();
  }

  public void addTsToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsToCount(this.contractState.getTsToCount() + 1)
        .build();
  }

  public void addTsToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsToCount(this.contractState.getTsToCount() + toAdd)
        .build();
  }

  public long getTsFromFee() {
    return this.getInstance().getTsFromFee();
  }

  public void addTsFromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsFromFee(this.contractState.getTsFromFee() + toAdd)
        .build();
  }

  public long getTsToFee() {
    return this.getInstance().getTsToFee();
  }

  public void addTsToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsToFee(this.contractState.getTsToFee() + toAdd)
        .build();
  }

  public long getTsFromEnergyUsage() {
    return this.getInstance().getTsFromEnergyUsage();
  }

  public void addTsFromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsFromEnergyUsage(this.contractState.getTsFromEnergyUsage() + toAdd)
        .build();
  }

  public long getTsToEnergyUsage() {
    return this.getInstance().getTsToEnergyUsage();
  }

  public void addTsToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsToEnergyUsage(this.contractState.getTsToEnergyUsage() + toAdd)
        .build();
  }

  public long getTsFromEnergyUsageTotal() {
    return this.getInstance().getTsFromEnergyUsageTotal();
  }

  public void addTsFromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsFromEnergyUsageTotal(this.contractState.getTsFromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsToEnergyUsageTotal() {
    return this.getInstance().getTsToEnergyUsageTotal();
  }

  public void addTsToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsToEnergyUsageTotal(this.contractState.getTsToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsfFromCount() {
    return this.getInstance().getTsfFromCount();
  }

  public void addTsfFromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsfFromCount(this.contractState.getTsfFromCount() + 1)
        .build();
  }

  public void addTsfFromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfFromCount(this.contractState.getTsfFromCount() + toAdd)
        .build();
  }

  public long getTsfToCount() {
    return this.getInstance().getTsfToCount();
  }

  public void addTsfToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsfToCount(this.contractState.getTsfToCount() + 1)
        .build();
  }

  public void addTsfToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfToCount(this.contractState.getTsfToCount() + toAdd)
        .build();
  }

  public long getTsfFromFee() {
    return this.getInstance().getTsfFromFee();
  }

  public void addTsfFromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfFromFee(this.contractState.getTsfFromFee() + toAdd)
        .build();
  }

  public long getTsfToFee() {
    return this.getInstance().getTsfToFee();
  }

  public void addTsfToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfToFee(this.contractState.getTsfToFee() + toAdd)
        .build();
  }

  public long getTsfFromEnergyUsage() {
    return this.getInstance().getTsfFromEnergyUsage();
  }

  public void addTsfFromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfFromEnergyUsage(this.contractState.getTsfFromEnergyUsage() + toAdd)
        .build();
  }

  public long getTsfToEnergyUsage() {
    return this.getInstance().getTsfToEnergyUsage();
  }

  public void addTsfToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfToEnergyUsage(this.contractState.getTsfToEnergyUsage() + toAdd)
        .build();
  }

  public long getTsfFromEnergyUsageTotal() {
    return this.getInstance().getTsfFromEnergyUsageTotal();
  }

  public void addTsfFromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfFromEnergyUsageTotal(this.contractState.getTsfFromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsfToEnergyUsageTotal() {
    return this.getInstance().getTsfToEnergyUsageTotal();
  }

  public void addTsfToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsfToEnergyUsageTotal(this.contractState.getTsfToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getCallerCount() {
    return this.getInstance().getCallerCount();
  }

  public void addCallerCount() {
    this.contractState = this.contractState.toBuilder()
        .setCallerCount(this.contractState.getCallerCount() + 1)
        .build();
  }

  public void addCallerCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setCallerCount(this.contractState.getCallerCount() + toAdd)
        .build();
  }

  public long getTriggerToCount() {
    return this.getInstance().getTriggerToCount();
  }

  public void addTriggerToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTriggerToCount(this.contractState.getTriggerToCount() + 1)
        .build();
  }

  public void addTriggerToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTriggerToCount(this.contractState.getTriggerToCount() + toAdd)
        .build();
  }

  public long getCallerFee() {
    return this.getInstance().getCallerFee();
  }

  public void addCallerFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setCallerFee(this.contractState.getCallerFee() + toAdd)
        .build();
  }

  public long getTriggerToFee() {
    return this.getInstance().getTriggerToFee();
  }

  public void addTriggerToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTriggerToFee(this.contractState.getTriggerToFee() + toAdd)
        .build();
  }

  public long getCallerEnergyUsage() {
    return this.getInstance().getCallerEnergyUsage();
  }

  public void addCallerEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setCallerEnergyUsage(this.contractState.getCallerEnergyUsage() + toAdd)
        .build();
  }

  public long getTriggerToEnergyUsage() {
    return this.getInstance().getTriggerToEnergyUsage();
  }

  public void addTriggerToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTriggerToEnergyUsage(this.contractState.getTriggerToEnergyUsage() + toAdd)
        .build();
  }

  public long getCallerEnergyUsageTotal() {
    return this.getInstance().getCallerEnergyUsageTotal();
  }

  public void addCallerEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setCallerEnergyUsageTotal(this.contractState.getCallerEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTriggerToEnergyUsageTotal() {
    return this.getInstance().getTriggerToEnergyUsageTotal();
  }

  public void addTriggerToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTriggerToEnergyUsageTotal(this.contractState.getTriggerToEnergyUsageTotal() + toAdd)
        .build();
  }

  public void addFromStats(
      BigInteger amount, long fee, long usage, long usageTotal, boolean isTransfer) {
    if (amount.compareTo(ONE_TRX) <= 0) {
      addTsSmallFromCount();
      addTsSmallFromFee(fee);
      addTsSmallFromEnergyUsage(usage);
      addTsSmallFromEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(TEN_TRX) <= 0) {
      addTs1To10FromCount();
      addTs1To10FromFee(fee);
      addTs1To10FromEnergyUsage(usage);
      addTs1To10FromEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(HUNDRED_TRX) <= 0) {
      addTs10To100FromCount();
      addTs10To100FromFee(fee);
      addTs10To100FromEnergyUsage(usage);
      addTs10To100FromEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(THOUSAND_TRX) <= 0) {
      addTs100To1000FromCount();
      addTs100To1000FromFee(fee);
      addTs100To1000FromEnergyUsage(usage);
      addTs100To1000FromEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(TEN_THOUSAND_TRX) <= 0) {
      addTs1000To10000FromCount();
      addTs1000To10000FromFee(fee);
      addTs1000To10000FromEnergyUsage(usage);
      addTs1000To10000FromEnergyUsageTotal(usageTotal);
    } else {
      addTsBigFromCount();
      addTsBigFromFee(fee);
      addTsBigFromEnergyUsage(usage);
      addTsBigFromEnergyUsageTotal(usageTotal);
    }
    if (!isTransfer) {
      addTsfFromCount();
      addTsfFromFee(fee);
      addTsfFromEnergyUsage(usage);
      addTsfFromEnergyUsageTotal(usageTotal);
    }
  }

  public void addTempFromStats(
      BigInteger amount, long usageTotal, boolean isTransfer) {
    if (amount.compareTo(ONE_TRX) <= 0) {
      addTsSmallFromCount();
      addTsSmallFromEnergyUsageTotal(usageTotal);
      addTsSmallFromNewCount();
    } else if (amount.compareTo(TEN_TRX) <= 0) {
      addTs1To10FromCount();
      addTs1To10FromEnergyUsageTotal(usageTotal);
      addTs1To10FromNewCount();
    } else if (amount.compareTo(HUNDRED_TRX) <= 0) {
      addTs10To100FromCount();
      addTs10To100FromEnergyUsageTotal(usageTotal);
      addTs10To100FromNewCount();
    } else if (amount.compareTo(THOUSAND_TRX) <= 0) {
      addTs100To1000FromCount();
      addTs100To1000FromEnergyUsageTotal(usageTotal);
      addTs100To1000FromNewCount();
    } else if (amount.compareTo(TEN_THOUSAND_TRX) <= 0) {
      addTs1000To10000FromCount();
      addTs1000To10000FromEnergyUsageTotal(usageTotal);
      addTs1000To10000FromNewCount();
    } else {
      addTsBigFromCount();
      addTsBigFromEnergyUsageTotal(usageTotal);
      addTsBigFromNewCount();
    }
    if (!isTransfer) {
      addTsfFromCount();
      addTsfFromEnergyUsageTotal(usageTotal);
    }
  }

  public void addToStats(
      BigInteger amount, long fee, long usage, long usageTotal, boolean isTransfer) {
    if (amount.compareTo(ONE_TRX) <= 0) {
      addTsSmallToCount();
      addTsSmallToFee(fee);
      addTsSmallToEnergyUsage(usage);
      addTsSmallToEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(TEN_TRX) <= 0) {
      addTs1To10ToCount();
      addTs1To10ToFee(fee);
      addTs1To10ToEnergyUsage(usage);
      addTs1To10ToEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(HUNDRED_TRX) <= 0) {
      addTs10To100ToCount();
      addTs10To100ToFee(fee);
      addTs10To100ToEnergyUsage(usage);
      addTs10To100ToEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(THOUSAND_TRX) <= 0) {
      addTs100To1000ToCount();
      addTs100To1000ToFee(fee);
      addTs100To1000ToEnergyUsage(usage);
      addTs100To1000ToEnergyUsageTotal(usageTotal);
    } else if (amount.compareTo(TEN_THOUSAND_TRX) <= 0) {
      addTs1000To10000ToCount();
      addTs1000To10000ToFee(fee);
      addTs1000To10000ToEnergyUsage(usage);
      addTs1000To10000ToEnergyUsageTotal(usageTotal);
    } else {
      addTsBigToCount();
      addTsBigToFee(fee);
      addTsBigToEnergyUsage(usage);
      addTsBigToEnergyUsageTotal(usageTotal);
    }
    if (!isTransfer) {
      addTsfToCount();
      addTsfToFee(fee);
      addTsfToEnergyUsage(usage);
      addTsfToEnergyUsageTotal(usageTotal);
    }
  }

  public void addTempToStats(
      BigInteger amount, long usageTotal, boolean isTransfer) {
    if (amount.compareTo(ONE_TRX) <= 0) {
      addTsSmallToCount();
      addTsSmallToEnergyUsageTotal(usageTotal);
      addTsSmallToNewCount();
    } else if (amount.compareTo(TEN_TRX) <= 0) {
      addTs1To10ToCount();
      addTs1To10ToEnergyUsageTotal(usageTotal);
      addTs1To10ToNewCount();
    } else if (amount.compareTo(HUNDRED_TRX) <= 0) {
      addTs10To100ToCount();
      addTs10To100ToEnergyUsageTotal(usageTotal);
      addTs10To100ToNewCount();
    } else if (amount.compareTo(THOUSAND_TRX) <= 0) {
      addTs100To1000ToCount();
      addTs100To1000ToEnergyUsageTotal(usageTotal);
      addTs100To1000ToNewCount();
    } else if (amount.compareTo(TEN_THOUSAND_TRX) <= 0) {
      addTs1000To10000ToCount();
      addTs1000To10000ToEnergyUsageTotal(usageTotal);
      addTs1000To10000ToNewCount();
    } else {
      addTsBigToCount();
      addTsBigToEnergyUsageTotal(usageTotal);
      addTsBigToNewCount();
    }
    if (!isTransfer) {
      addTsfToCount();
      addTsfToEnergyUsageTotal(usageTotal);
    }
  }

  public long getTsSmallFromCount() {
    return this.getInstance().getTsSmallFromCount();
  }

  public void addTsSmallFromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallFromCount(this.contractState.getTsSmallFromCount() + 1)
        .build();
  }

  public void addTsSmallFromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallFromCount(this.contractState.getTsSmallFromCount() + toAdd)
        .build();
  }

  public long getTsSmallToCount() {
    return this.getInstance().getTsSmallToCount();
  }

  public void addTsSmallToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallToCount(this.contractState.getTsSmallToCount() + 1)
        .build();
  }

  public void addTsSmallToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallToCount(this.contractState.getTsSmallToCount() + toAdd)
        .build();
  }

  public long getTsSmallFromFee() {
    return this.getInstance().getTsSmallFromFee();
  }

  public void addTsSmallFromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallFromFee(this.contractState.getTsSmallFromFee() + toAdd)
        .build();
  }

  public long getTsSmallToFee() {
    return this.getInstance().getTsSmallToFee();
  }

  public void addTsSmallToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallToFee(this.contractState.getTsSmallToFee() + toAdd)
        .build();
  }

  public long getTsSmallFromEnergyUsage() {
    return this.getInstance().getTsSmallFromEnergyUsage();
  }

  public void addTsSmallFromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallFromEnergyUsage(this.contractState.getTsSmallFromEnergyUsage() + toAdd)
        .build();
  }

  public long getTsSmallToEnergyUsage() {
    return this.getInstance().getTsSmallToEnergyUsage();
  }

  public void addTsSmallToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallToEnergyUsage(this.contractState.getTsSmallToEnergyUsage() + toAdd)
        .build();
  }

  public long getTsSmallFromEnergyUsageTotal() {
    return this.getInstance().getTsSmallFromEnergyUsageTotal();
  }

  public void addTsSmallFromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallFromEnergyUsageTotal(this.contractState.getTsSmallFromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsSmallToEnergyUsageTotal() {
    return this.getInstance().getTsSmallToEnergyUsageTotal();
  }

  public void addTsSmallToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallToEnergyUsageTotal(this.contractState.getTsSmallToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs1To10FromCount() {
    return this.getInstance().getTs1To10FromCount();
  }

  public void addTs1To10FromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10FromCount(this.contractState.getTs1To10FromCount() + 1)
        .build();
  }

  public void addTs1To10FromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10FromCount(this.contractState.getTs1To10FromCount() + toAdd)
        .build();
  }

  public long getTs1To10ToCount() {
    return this.getInstance().getTs1To10ToCount();
  }

  public void addTs1To10ToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10ToCount(this.contractState.getTs1To10ToCount() + 1)
        .build();
  }

  public void addTs1To10ToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10ToCount(this.contractState.getTs1To10ToCount() + toAdd)
        .build();
  }

  public long getTs1To10FromFee() {
    return this.getInstance().getTs1To10FromFee();
  }

  public void addTs1To10FromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10FromFee(this.contractState.getTs1To10FromFee() + toAdd)
        .build();
  }

  public long getTs1To10ToFee() {
    return this.getInstance().getTs1To10ToFee();
  }

  public void addTs1To10ToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10ToFee(this.contractState.getTs1To10ToFee() + toAdd)
        .build();
  }

  public long getTs1To10FromEnergyUsage() {
    return this.getInstance().getTs1To10FromEnergyUsage();
  }

  public void addTs1To10FromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10FromEnergyUsage(this.contractState.getTs1To10FromEnergyUsage() + toAdd)
        .build();
  }

  public long getTs1To10ToEnergyUsage() {
    return this.getInstance().getTs1To10ToEnergyUsage();
  }

  public void addTs1To10ToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10ToEnergyUsage(this.contractState.getTs1To10ToEnergyUsage() + toAdd)
        .build();
  }

  public long getTs1To10FromEnergyUsageTotal() {
    return this.getInstance().getTs1To10FromEnergyUsageTotal();
  }

  public void addTs1To10FromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10FromEnergyUsageTotal(this.contractState.getTs1To10FromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs1To10ToEnergyUsageTotal() {
    return this.getInstance().getTs1To10ToEnergyUsageTotal();
  }

  public void addTs1To10ToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10ToEnergyUsageTotal(this.contractState.getTs1To10ToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs10To100FromCount() {
    return this.getInstance().getTs10To100FromCount();
  }

  public void addTs10To100FromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100FromCount(this.contractState.getTs10To100FromCount() + 1)
        .build();
  }

  public void addTs10To100FromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100FromCount(this.contractState.getTs10To100FromCount() + toAdd)
        .build();
  }

  public long getTs10To100ToCount() {
    return this.getInstance().getTs10To100ToCount();
  }

  public void addTs10To100ToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100ToCount(this.contractState.getTs10To100ToCount() + 1)
        .build();
  }

  public void addTs10To100ToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100ToCount(this.contractState.getTs10To100ToCount() + toAdd)
        .build();
  }

  public long getTs10To100FromFee() {
    return this.getInstance().getTs10To100FromFee();
  }

  public void addTs10To100FromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100FromFee(this.contractState.getTs10To100FromFee() + toAdd)
        .build();
  }

  public long getTs10To100ToFee() {
    return this.getInstance().getTs10To100ToFee();
  }

  public void addTs10To100ToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100ToFee(this.contractState.getTs10To100ToFee() + toAdd)
        .build();
  }

  public long getTs10To100FromEnergyUsage() {
    return this.getInstance().getTs10To100FromEnergyUsage();
  }

  public void addTs10To100FromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100FromEnergyUsage(this.contractState.getTs10To100FromEnergyUsage() + toAdd)
        .build();
  }

  public long getTs10To100ToEnergyUsage() {
    return this.getInstance().getTs10To100ToEnergyUsage();
  }

  public void addTs10To100ToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100ToEnergyUsage(this.contractState.getTs10To100ToEnergyUsage() + toAdd)
        .build();
  }

  public long getTs10To100FromEnergyUsageTotal() {
    return this.getInstance().getTs10To100FromEnergyUsageTotal();
  }

  public void addTs10To100FromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100FromEnergyUsageTotal(this.contractState.getTs10To100FromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs10To100ToEnergyUsageTotal() {
    return this.getInstance().getTs10To100ToEnergyUsageTotal();
  }

  public void addTs10To100ToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs10To100ToEnergyUsageTotal(this.contractState.getTs10To100ToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs100To1000FromCount() {
    return this.getInstance().getTs100To1000FromCount();
  }

  public void addTs100To1000FromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromCount(this.contractState.getTs100To1000FromCount() + 1)
        .build();
  }

  public void addTs100To1000FromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromCount(this.contractState.getTs100To1000FromCount() + toAdd)
        .build();
  }

  public long getTs100To1000ToCount() {
    return this.getInstance().getTs100To1000ToCount();
  }

  public void addTs100To1000ToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToCount(this.contractState.getTs100To1000ToCount() + 1)
        .build();
  }

  public void addTs100To1000ToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToCount(this.contractState.getTs100To1000ToCount() + toAdd)
        .build();
  }

  public long getTs100To1000FromFee() {
    return this.getInstance().getTs100To1000FromFee();
  }

  public void addTs100To1000FromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromFee(this.contractState.getTs100To1000FromFee() + toAdd)
        .build();
  }

  public long getTs100To1000ToFee() {
    return this.getInstance().getTs100To1000ToFee();
  }

  public void addTs100To1000ToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToFee(this.contractState.getTs100To1000ToFee() + toAdd)
        .build();
  }

  public long getTs100To1000FromEnergyUsage() {
    return this.getInstance().getTs100To1000FromEnergyUsage();
  }

  public void addTs100To1000FromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromEnergyUsage(this.contractState.getTs100To1000FromEnergyUsage() + toAdd)
        .build();
  }

  public long getTs100To1000ToEnergyUsage() {
    return this.getInstance().getTs100To1000ToEnergyUsage();
  }

  public void addTs100To1000ToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToEnergyUsage(this.contractState.getTs100To1000ToEnergyUsage() + toAdd)
        .build();
  }

  public long getTs100To1000FromEnergyUsageTotal() {
    return this.getInstance().getTs100To1000FromEnergyUsageTotal();
  }

  public void addTs100To1000FromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromEnergyUsageTotal(this.contractState.getTs100To1000FromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs100To1000ToEnergyUsageTotal() {
    return this.getInstance().getTs100To1000ToEnergyUsageTotal();
  }

  public void addTs100To1000ToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToEnergyUsageTotal(this.contractState.getTs100To1000ToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs1000To10000FromCount() {
    return this.getInstance().getTs1000To10000FromCount();
  }

  public void addTs1000To10000FromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000FromCount(this.contractState.getTs1000To10000FromCount() + 1)
        .build();
  }

  public void addTs1000To10000FromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000FromCount(this.contractState.getTs1000To10000FromCount() + toAdd)
        .build();
  }

  public long getTs1000To10000ToCount() {
    return this.getInstance().getTs1000To10000ToCount();
  }

  public void addTs1000To10000ToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000ToCount(this.contractState.getTs1000To10000ToCount() + 1)
        .build();
  }

  public void addTs1000To10000ToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000ToCount(this.contractState.getTs1000To10000ToCount() + toAdd)
        .build();
  }

  public long getTs1000To10000FromFee() {
    return this.getInstance().getTs1000To10000FromFee();
  }

  public void addTs1000To10000FromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000FromFee(this.contractState.getTs1000To10000FromFee() + toAdd)
        .build();
  }

  public long getTs1000To10000ToFee() {
    return this.getInstance().getTs1000To10000ToFee();
  }

  public void addTs1000To10000ToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000ToFee(this.contractState.getTs1000To10000ToFee() + toAdd)
        .build();
  }

  public long getTs1000To10000FromEnergyUsage() {
    return this.getInstance().getTs1000To10000FromEnergyUsage();
  }

  public void addTs1000To10000FromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000FromEnergyUsage(this.contractState.getTs1000To10000FromEnergyUsage() + toAdd)
        .build();
  }

  public long getTs1000To10000ToEnergyUsage() {
    return this.getInstance().getTs1000To10000ToEnergyUsage();
  }

  public void addTs1000To10000ToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000ToEnergyUsage(this.contractState.getTs1000To10000ToEnergyUsage() + toAdd)
        .build();
  }

  public long getTs1000To10000FromEnergyUsageTotal() {
    return this.getInstance().getTs1000To10000FromEnergyUsageTotal();
  }

  public void addTs1000To10000FromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000FromEnergyUsageTotal(this.contractState.getTs1000To10000FromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTs1000To10000ToEnergyUsageTotal() {
    return this.getInstance().getTs1000To10000ToEnergyUsageTotal();
  }

  public void addTs1000To10000ToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000ToEnergyUsageTotal(this.contractState.getTs1000To10000ToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsBigFromCount() {
    return this.getInstance().getTsBigFromCount();
  }

  public void addTsBigFromCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsBigFromCount(this.contractState.getTsBigFromCount() + 1)
        .build();
  }

  public void addTsBigFromCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigFromCount(this.contractState.getTsBigFromCount() + toAdd)
        .build();
  }

  public long getTsBigToCount() {
    return this.getInstance().getTsBigToCount();
  }

  public void addTsBigToCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsBigToCount(this.contractState.getTsBigToCount() + 1)
        .build();
  }

  public void addTsBigToCount(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigToCount(this.contractState.getTsBigToCount() + toAdd)
        .build();
  }

  public long getTsBigFromFee() {
    return this.getInstance().getTsBigFromFee();
  }

  public void addTsBigFromFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigFromFee(this.contractState.getTsBigFromFee() + toAdd)
        .build();
  }

  public long getTsBigToFee() {
    return this.getInstance().getTsBigToFee();
  }

  public void addTsBigToFee(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigToFee(this.contractState.getTsBigToFee() + toAdd)
        .build();
  }

  public long getTsBigFromEnergyUsage() {
    return this.getInstance().getTsBigFromEnergyUsage();
  }

  public void addTsBigFromEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigFromEnergyUsage(this.contractState.getTsBigFromEnergyUsage() + toAdd)
        .build();
  }

  public long getTsBigToEnergyUsage() {
    return this.getInstance().getTsBigToEnergyUsage();
  }

  public void addTsBigToEnergyUsage(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigToEnergyUsage(this.contractState.getTsBigToEnergyUsage() + toAdd)
        .build();
  }

  public long getTsBigFromEnergyUsageTotal() {
    return this.getInstance().getTsBigFromEnergyUsageTotal();
  }

  public void addTsBigFromEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigFromEnergyUsageTotal(this.contractState.getTsBigFromEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsBigToEnergyUsageTotal() {
    return this.getInstance().getTsBigToEnergyUsageTotal();
  }

  public void addTsBigToEnergyUsageTotal(long toAdd) {
    this.contractState = this.contractState.toBuilder()
        .setTsBigToEnergyUsageTotal(this.contractState.getTsBigToEnergyUsageTotal() + toAdd)
        .build();
  }

  public long getTsSmallFromNewCount() {
    return this.getInstance().getTsSmallFromNewCount();
  }

  public void addTsSmallFromNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallFromNewCount(this.contractState.getTsSmallFromNewCount() + 1)
        .build();
  }

  public long getTs1To10FromNewCount() {
    return this.getInstance().getTs1To10FromNewCount();
  }

  public void addTs1To10FromNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10FromNewCount(this.contractState.getTs1To10FromNewCount() + 1)
        .build();
  }

  public long getTs10To100FromNewCount() {
    return this.getInstance().getTs10To100FromNewCount();
  }

  public void addTs10To100FromNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromNewCount(this.contractState.getTs10To100FromNewCount() + 1)
        .build();
  }

  public long getTs100To1000FromNewCount() {
    return this.getInstance().getTs100To1000FromNewCount();
  }

  public void addTs100To1000FromNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000FromNewCount(this.contractState.getTs100To1000FromNewCount() + 1)
        .build();
  }

  public long getTs1000To10000FromNewCount() {
    return this.getInstance().getTs1000To10000FromNewCount();
  }

  public void addTs1000To10000FromNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000FromNewCount(this.contractState.getTs1000To10000FromNewCount() + 1)
        .build();
  }

  public long getTsBigFromNewCount() {
    return this.getInstance().getTsBigFromNewCount();
  }

  public void addTsBigFromNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsBigFromNewCount(this.contractState.getTsBigFromNewCount() + 1)
        .build();
  }

  public long getTsSmallToNewCount() {
    return this.getInstance().getTsSmallToNewCount();
  }

  public void addTsSmallToNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsSmallToNewCount(this.contractState.getTsSmallToNewCount() + 1)
        .build();
  }

  public long getTs1To10ToNewCount() {
    return this.getInstance().getTs1To10ToNewCount();
  }

  public void addTs1To10ToNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1To10ToNewCount(this.contractState.getTs1To10ToNewCount() + 1)
        .build();
  }

  public long getTs10To100ToNewCount() {
    return this.getInstance().getTs10To100ToNewCount();
  }

  public void addTs10To100ToNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToNewCount(this.contractState.getTs10To100ToNewCount() + 1)
        .build();
  }

  public long getTs100To1000ToNewCount() {
    return this.getInstance().getTs100To1000ToNewCount();
  }

  public void addTs100To1000ToNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs100To1000ToNewCount(this.contractState.getTs100To1000ToNewCount() + 1)
        .build();
  }

  public long getTs1000To10000ToNewCount() {
    return this.getInstance().getTs1000To10000ToNewCount();
  }

  public void addTs1000To10000ToNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTs1000To10000ToNewCount(this.contractState.getTs1000To10000ToNewCount() + 1)
        .build();
  }

  public long getTsBigToNewCount() {
    return this.getInstance().getTsBigToNewCount();
  }

  public void addTsBigToNewCount() {
    this.contractState = this.contractState.toBuilder()
        .setTsBigToNewCount(this.contractState.getTsBigToNewCount() + 1)
        .build();
  }

  public void clearTempEnergyRecord() {
    this.contractState = this.contractState.toBuilder()
        .setTransferNewEnergyUsage(0)
        .setTransferFromNewEnergyUsage(0)
        .setTsSmallFromNewCount(0)
        .setTs1To10FromNewCount(0)
        .setTs10To100FromNewCount(0)
        .setTs100To1000FromNewCount(0)
        .setTs1000To10000FromNewCount(0)
        .setTsBigFromNewCount(0)
        .setTsSmallToNewCount(0)
        .setTs1To10ToNewCount(0)
        .setTs10To100ToNewCount(0)
        .setTs100To1000ToNewCount(0)
        .setTs1000To10000ToNewCount(0)
        .setTsBigToNewCount(0)
        .build();
  }

  public long getInternalTransferCount() {
    return getTsSmallFromNewCount()
            + getTs1To10FromNewCount()
            + getTs10To100FromNewCount()
            + getTs100To1000FromNewCount()
            + getTs1000To10000FromNewCount()
            + getTsBigFromNewCount()
            + getTsSmallToNewCount()
            + getTs1To10ToNewCount()
            + getTs10To100ToNewCount()
            + getTs100To1000ToNewCount()
            + getTs1000To10000ToNewCount()
            + getTsBigToNewCount();
  }

  public void updateInternalEnergyFee(long newFee, long sumCount) {
    if (getTsSmallFromNewCount() > 0) {
      addTsSmallFromFee((long) ((double) getTsSmallFromNewCount() * newFee / sumCount));
    }
    if (getTs1To10FromNewCount() > 0) {
      addTs1To10FromFee((long) ((double) getTs1To10FromNewCount() * newFee / sumCount));
    }
    if (getTs10To100FromNewCount() > 0) {
      addTs10To100FromFee((long) ((double) getTs10To100FromNewCount() * newFee / sumCount));
    }
    if (getTs100To1000FromNewCount() > 0) {
      addTs100To1000FromFee((long) ((double) getTs100To1000FromNewCount() * newFee / sumCount));
    }
    if (getTs1000To10000FromNewCount() > 0) {
      addTs1000To10000FromFee((long) ((double) getTs1000To10000FromNewCount() * newFee / sumCount));
    }
    if (getTsBigFromNewCount() > 0) {
      addTsBigFromFee((long) ((double) getTsBigFromNewCount() * newFee / sumCount));
    }
    if (getTsSmallToNewCount() > 0) {
      addTsSmallToFee((long) ((double) getTsSmallToNewCount() * newFee / sumCount));
    }
    if (getTs1To10ToNewCount() > 0) {
      addTs1To10ToFee((long) ((double) getTs1To10ToNewCount() * newFee / sumCount));
    }
    if (getTs10To100ToNewCount() > 0) {
      addTs10To100ToFee((long) ((double) getTs10To100ToNewCount() * newFee / sumCount));
    }
    if (getTs100To1000ToNewCount() > 0) {
      addTs100To1000ToFee((long) ((double) getTs100To1000ToNewCount() * newFee / sumCount));
    }
    if (getTs1000To10000ToNewCount() > 0) {
      addTs1000To10000ToFee((long) ((double) getTs1000To10000ToNewCount() * newFee / sumCount));
    }
    if (getTsBigToNewCount() > 0) {
      addTsBigToFee((long) ((double) getTsBigToNewCount() * newFee / sumCount));
    }
  }

  public boolean catchUpToCycle(DynamicPropertiesStore dps) {
    return catchUpToCycle(
        dps.getCurrentCycleNumber(),
        dps.getDynamicEnergyThreshold(),
        dps.getDynamicEnergyIncreaseFactor(),
        dps.getDynamicEnergyMaxFactor()
    );
  }

  public boolean catchUpToCycle(
      long newCycle, long threshold, long increaseFactor, long maxFactor
  ) {
    long lastCycle = getUpdateCycle();

    // Updated within this cycle
    if (lastCycle == newCycle) {
      return false;
    }

    // Guard judge and uninitialized state
    if (lastCycle > newCycle || lastCycle == 0L) {
      reset(newCycle);
      return true;
    }

    final long precisionFactor = DYNAMIC_ENERGY_FACTOR_DECIMAL;

    // Increase the last cycle
    // fix the threshold = 0 caused incompatible
    if (getEnergyUsage() > threshold) {
      lastCycle += 1;
      double increasePercent = 1 + (double) increaseFactor / precisionFactor;
      this.contractState = ContractState.newBuilder()
          .setUpdateCycle(lastCycle)
          .setEnergyFactor(Math.min(
              maxFactor,
              (long) ((getEnergyFactor() + precisionFactor) * increasePercent) - precisionFactor))
          .build();
    }

    // No need to decrease
    long cycleCount = newCycle - lastCycle;
    if (cycleCount <= 0) {
      return true;
    }

    // Calc the decrease percent (decrease factor [75% ~ 100%])
    double decreasePercent = Math.pow(
        1 - (double) increaseFactor / DYNAMIC_ENERGY_DECREASE_DIVISION / precisionFactor,
        cycleCount
    );

    // Decrease to this cycle
    // (If long time no tx and factor is 100%,
    //  we just calc it again and result factor is still 100%.
    //  That means we merge this special case to normal cases)
    this.contractState = ContractState.newBuilder()
        .setUpdateCycle(newCycle)
        .setEnergyFactor(Math.max(
            0,
            (long) ((getEnergyFactor() + precisionFactor) * decreasePercent) - precisionFactor))
        .build();

    return true;
  }

  public void merge(ContractStateCapsule other)
      throws IllegalAccessException, NoSuchFieldException {
    if (other == null) {
      return;
    }
    
    addEnergyUsageTotal(other.getEnergyUsageTotal());
    addEnergyUsageFailed(other.getEnergyUsageFailed());
    addEnergyPenaltyTotal(other.getEnergyPenaltyTotal());
    addEnergyPenaltyFailed(other.getEnergyPenaltyFailed());
    addTrxBurn(other.getTrxBurn());
    addTrxPenalty(other.getTrxPenalty());
    addTxTotalCount(other.getTxTotalCount());
    addTxFailedCount(other.getTxFailedCount());
    addTransferCount(other.getTransferCount());
    addTransferFromCount(other.getTransferFromCount());
    addTransferFee(other.getTransferFee());
    addTransferFromFee(other.getTransferFromFee());
    addTransferEnergyUsage(other.getTransferEnergyUsage());
    addTransferEnergyPenalty(other.getTransferEnergyPenalty());
    addTransferFromEnergyUsage(other.getTransferFromEnergyUsage());
    addTransferFromEnergyPenalty(other.getTransferFromEnergyPenalty());
    // transfer
    addTsFromCount(other.getTsFromCount());
    addTsToCount(other.getTsToCount());
    addTsFromFee(other.getTsFromFee());
    addTsToFee(other.getTsToFee());
    addTsFromEnergyUsage(other.getTsFromEnergyUsage());
    addTsToEnergyUsage(other.getTsToEnergyUsage());
    addTsFromEnergyUsageTotal(other.getTsFromEnergyUsageTotal());
    addTsToEnergyUsageTotal(other.getTsToEnergyUsageTotal());
    // transfer from
    addTsfFromCount(other.getTsfFromCount());
    addTsfToCount(other.getTsfToCount());
    addTsfFromFee(other.getTsfFromFee());
    addTsfToFee(other.getTsfToFee());
    addTsfFromEnergyUsage(other.getTsfFromEnergyUsage());
    addTsfToEnergyUsage(other.getTsfToEnergyUsage());
    addTsfFromEnergyUsageTotal(other.getTsfFromEnergyUsageTotal());
    addTsfToEnergyUsageTotal(other.getTsfToEnergyUsageTotal());
    // caller and trigger
    addCallerCount(other.getCallerCount());
    addTriggerToCount(other.getTriggerToCount());
    addCallerFee(other.getCallerFee());
    addTriggerToFee(other.getTriggerToFee());
    addCallerEnergyUsage(other.getCallerEnergyUsage());
    addTriggerToEnergyUsage(other.getTriggerToEnergyUsage());
    addCallerEnergyUsageTotal(other.getCallerEnergyUsageTotal());
    addTriggerToEnergyUsageTotal(other.getTriggerToEnergyUsageTotal());
    // transfer range
    addTsSmallFromCount(other.getTsSmallFromCount());
    addTsSmallToCount(other.getTsSmallToCount());
    addTsSmallFromFee(other.getTsSmallFromFee());
    addTsSmallToFee(other.getTsSmallToFee());
    addTsSmallFromEnergyUsage(other.getTsSmallFromEnergyUsage());
    addTsSmallToEnergyUsage(other.getTsSmallToEnergyUsage());
    addTsSmallFromEnergyUsageTotal(other.getTsSmallFromEnergyUsageTotal());
    addTsSmallToEnergyUsageTotal(other.getTsSmallToEnergyUsageTotal());

    addTs1To10FromCount(other.getTs1To10FromCount());
    addTs1To10ToCount(other.getTs1To10ToCount());
    addTs1To10FromFee(other.getTs1To10FromFee());
    addTs1To10ToFee(other.getTs1To10ToFee());
    addTs1To10FromEnergyUsage(other.getTs1To10FromEnergyUsage());
    addTs1To10ToEnergyUsage(other.getTs1To10ToEnergyUsage());
    addTs1To10FromEnergyUsageTotal(other.getTs1To10FromEnergyUsageTotal());
    addTs1To10ToEnergyUsageTotal(other.getTs1To10ToEnergyUsageTotal());

    addTs10To100FromCount(other.getTs10To100FromCount());
    addTs10To100ToCount(other.getTs10To100ToCount());
    addTs10To100FromFee(other.getTs10To100FromFee());
    addTs10To100ToFee(other.getTs10To100ToFee());
    addTs10To100FromEnergyUsage(other.getTs10To100FromEnergyUsage());
    addTs10To100ToEnergyUsage(other.getTs10To100ToEnergyUsage());
    addTs10To100FromEnergyUsageTotal(other.getTs10To100FromEnergyUsageTotal());
    addTs10To100ToEnergyUsageTotal(other.getTs10To100ToEnergyUsageTotal());

    addTs100To1000FromCount(other.getTs100To1000FromCount());
    addTs100To1000ToCount(other.getTs100To1000ToCount());
    addTs100To1000FromFee(other.getTs100To1000FromFee());
    addTs100To1000ToFee(other.getTs100To1000ToFee());
    addTs100To1000FromEnergyUsage(other.getTs100To1000FromEnergyUsage());
    addTs100To1000ToEnergyUsage(other.getTs100To1000ToEnergyUsage());
    addTs100To1000FromEnergyUsageTotal(other.getTs100To1000FromEnergyUsageTotal());
    addTs100To1000ToEnergyUsageTotal(other.getTs100To1000ToEnergyUsageTotal());

    addTs1000To10000FromCount(other.getTs1000To10000FromCount());
    addTs1000To10000ToCount(other.getTs1000To10000ToCount());
    addTs1000To10000FromFee(other.getTs1000To10000FromFee());
    addTs1000To10000ToFee(other.getTs1000To10000ToFee());
    addTs1000To10000FromEnergyUsage(other.getTs1000To10000FromEnergyUsage());
    addTs1000To10000ToEnergyUsage(other.getTs1000To10000ToEnergyUsage());
    addTs1000To10000FromEnergyUsageTotal(other.getTs1000To10000FromEnergyUsageTotal());
    addTs1000To10000ToEnergyUsageTotal(other.getTs1000To10000ToEnergyUsageTotal());

    addTsBigFromCount(other.getTsBigFromCount());
    addTsBigToCount(other.getTsBigToCount());
    addTsBigFromFee(other.getTsBigFromFee());
    addTsBigToFee(other.getTsBigToFee());
    addTsBigFromEnergyUsage(other.getTsBigFromEnergyUsage());
    addTsBigToEnergyUsage(other.getTsBigToEnergyUsage());
    addTsBigFromEnergyUsageTotal(other.getTsBigFromEnergyUsageTotal());
    addTsBigToEnergyUsageTotal(other.getTsBigToEnergyUsageTotal());
  }

  public long getTsTotalFromCount() {
    return getTsSmallFromCount()
        + getTs1To10FromCount()
        + getTs10To100FromCount()
        + getTs100To1000FromCount()
        + getTs1000To10000FromCount()
        + getTsBigFromCount();
  }

  public long getTsTotalFromFee() {
    return getTsSmallFromFee()
        + getTs1To10FromFee()
        + getTs10To100FromFee()
        + getTs100To1000FromFee()
        + getTs1000To10000FromFee()
        + getTsBigFromFee();
  }

  public long getTsTotalFromEnergyUsage() {
    return getTsSmallFromEnergyUsage()
        + getTs1To10FromEnergyUsage()
        + getTs10To100FromEnergyUsage()
        + getTs100To1000FromEnergyUsage()
        + getTs1000To10000FromEnergyUsage()
        + getTsBigFromEnergyUsage();
  }

  public long getTsTotalToCount() {
    return getTsSmallToCount()
        + getTs1To10ToCount()
        + getTs10To100ToCount()
        + getTs100To1000ToCount()
        + getTs1000To10000ToCount()
        + getTsBigToCount();
  }

  public long getTsTotalToFee() {
    return getTsSmallToFee()
        + getTs1To10ToFee()
        + getTs10To100ToFee()
        + getTs100To1000ToFee()
        + getTs1000To10000ToFee()
        + getTsBigToFee();
  }

  public long getTsTotalToEnergyUsage() {
    return getTsSmallToEnergyUsage()
        + getTs1To10ToEnergyUsage()
        + getTs10To100ToEnergyUsage()
        + getTs100To1000ToEnergyUsage()
        + getTs1000To10000ToEnergyUsage()
        + getTsBigToEnergyUsage();
  }

  public String getUsdtOutput() {
    return "{\n"
        + "\"transfer_count\":"
        + getTransferCount()
        + ", \"transfer_fee\":"
        + getTransferFee()
        + ", \"transfer_energy_usage\":"
        + getTransferEnergyUsage()
        + ", \"transfer_from_count\":"
        + getTransferFromCount()
        + ", \"transfer_from_fee\":"
        + getTransferFromFee()
        + ", \"transfer_from_energy_usage\":"
        + getTransferFromEnergyUsage()
        + ", \"average_trx\":"
        + ((double) (getTransferFee() + getTransferFromFee())
            / (getTransferCount() + getTransferFromCount()))
        + "}";
  }

  public String getTriggerOutput() {
    return "{\n"
        + "\"transfer_count\":"
        + getTriggerToCount()
        + ", \"transfer_fee\":"
        + getTriggerToFee()
        + ", \"transfer_energy_usage\":"
        + getTriggerToEnergyUsage()
        + ", \"transfer_energy_usage_total\":"
        + getTriggerToEnergyUsageTotal()
        + ", \"average_trx\":"
        + ((double) getTriggerToFee()
        / getTriggerToCount())
        + "}";
  }

  public String getTransferFromToOutput() {
    return "{\n"
        + "\"transfer_from_count\":"
        + getTsTotalFromCount()
        + ", \"transfer_from_fee\":"
        + getTsTotalFromFee()
        + ", \"transfer_from_energy_usage\":"
        + getTsTotalFromEnergyUsage()
        + ", \"from_average_trx\":"
        + ((double) getTsTotalFromFee()
        / getTsTotalFromCount())
        + ", \"transfer_to_count\":"
        + getTsTotalToCount()
        + ", \"transfer_to_fee\":"
        + getTsTotalToFee()
        + ", \"transfer_to_energy_usage\":"
        + getTsTotalToEnergyUsage()
        + ", \"to_average_trx\":"
        + ((double) getTsTotalToFee()
        / getTsTotalToCount())
        + ", \"transfer_count\":"
        + (getTsTotalFromCount() + getTsTotalToCount())
        + ", \"transfer_fee\":"
        + (getTsTotalFromFee() + getTsTotalToFee())
        + ", \"transfer_energy_usage\":"
        + (getTsTotalFromEnergyUsage() + getTsTotalToEnergyUsage())
        + ", \"average_trx\":"
        + ((double) (getTsTotalFromFee() + getTsTotalToFee())
        / (getTsTotalFromCount() + getTsTotalToCount()))
        + "}";
  }

  public void reset(long latestCycle) {
    this.contractState = ContractState.newBuilder()
        .setUpdateCycle(latestCycle)
        .build();
  }

  @Override
  public String toString() {
    return "{\n" + contractState.toString() + '}';
  }
}
