package com.hbjycl.module;

/**
 * Created by Admin on 2016/11/23.
 */

/**
 * 应答码
 */
public class ReplyCode {
    private String length;
    private String carId;
    private String version;
    private String commandCode;
    private String commandId;
    private String reserved;
    private String terminalCommandCode;
    private String terminalCommandId;
    private String result;

    @Override
    public String toString() {
        return length+carId+version+commandCode+commandId+reserved +terminalCommandCode+terminalCommandId+result;
    }

    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public String getCarId() {
        return carId;
    }

    public void setCarId(String carId) {
        this.carId = carId;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCommandCode() {
        return commandCode;
    }

    public void setCommandCode(String commandCode) {
        this.commandCode = commandCode;
    }

    public String getCommandId() {
        return commandId;
    }

    public void setCommandId(String commandId) {
        this.commandId = commandId;
    }

    public String getReserved() {
        return reserved;
    }

    public void setReserved(String reserved) {
        this.reserved = reserved;
    }

    public String getTerminalCommandCode() {
        return terminalCommandCode;
    }

    public void setTerminalCommandCode(String terminalCommandCode) {
        this.terminalCommandCode = terminalCommandCode;
    }

    public String getTerminalCommandId() {
        return terminalCommandId;
    }

    public void setTerminalCommandId(String terminalCommandId) {
        this.terminalCommandId = terminalCommandId;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }


}
