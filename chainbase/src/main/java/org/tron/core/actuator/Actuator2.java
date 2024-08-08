package org.tron.core.actuator;

import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;

public interface Actuator2 {

  void execute(Object object) throws ContractExeException;
  void execute(Object object, boolean check) throws ContractExeException;

  void validate(Object object) throws ContractValidateException;
  void validate(Object object, boolean check) throws ContractValidateException;
}