package org.tron.common.runtime;

import org.tron.core.db.TransactionContext;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;

import java.util.List;


public interface Runtime {

  void execute(TransactionContext context)
      throws ContractValidateException, ContractExeException;

  ProgramResult getResult();

  String getRuntimeError();

}
