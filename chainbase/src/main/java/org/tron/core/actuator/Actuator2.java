package org.tron.core.actuator;

import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;

import java.util.List;

public interface Actuator2 {

  void execute(Object object) throws ContractExeException;

  void validate(Object object) throws ContractValidateException;

  List<byte[]> getAllAccountKeys();
}