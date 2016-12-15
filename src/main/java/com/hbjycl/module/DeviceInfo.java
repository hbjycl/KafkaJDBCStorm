package com.hbjycl.module;

/**
 * Created by Admin on 2016/11/30.
 * 工况
 */
public class DeviceInfo {
    private String length;
    private String cusID;
    private String cardID;
    private String version;
    private String commandCode;
    private String commandId;
    private String reserved;
    private String longitude;//经度
    private String latitude;//纬度
    private String speed;//速度
    private String direction;//方向
    private String height;//海拔高度
    private String statusBits;//状态位
    private String date;//日期
    private String time;//时间
    private String gsm;//GSM信号强度
    private String gps;//gps卫星颗数
    private String runTime;//运行时间
    private String runMile;//运行里程
    private String totalMile;//累积里程
    private String runSpeed;//运行车速
    private String power;//功率
    private String fuel;//实时油耗(小时燃油消耗率)
    private String totalFuel;//累计油耗
    private String malfunction;//故障
    private String warnType;//报警类型
    private String OnOff;//开关机
    private String content;

    @Override
    public String toString() {
        return "DeviceInfo{" +
                "length='" + length + '\'' +
                ", cusID='" + cusID + '\'' +
                ", cardID='" + cardID + '\'' +
                ", version='" + version + '\'' +
                ", commandCode='" + commandCode + '\'' +
                ", commandId='" + commandId + '\'' +
                ", reserved='" + reserved + '\'' +
                ", longitude='" + longitude + '\'' +
                ", latitude='" + latitude + '\'' +
                ", speed='" + speed + '\'' +
                ", direction='" + direction + '\'' +
                ", height='" + height + '\'' +
                ", statusBits='" + statusBits + '\'' +
                ", date='" + date + '\'' +
                ", time='" + time + '\'' +
                ", gsm='" + gsm + '\'' +
                ", gps='" + gps + '\'' +
                ", runTime='" + runTime + '\'' +
                ", runMile='" + runMile + '\'' +
                ", totalMile='" + totalMile + '\'' +
                ", runSpeed='" + runSpeed + '\'' +
                ", power='" + power + '\'' +
                ", fuel='" + fuel + '\'' +
                ", totalFuel='" + totalFuel + '\'' +
                ", malfunction='" + malfunction + '\'' +
                ", warnType='" + warnType + '\'' +
                ", OnOff='" + OnOff + '\'' +
                ", content='" + content + '\'' +
                '}';
    }

    public String getOnOff() {
        return OnOff;
    }

    public void setOnOff(String onOff) {
        OnOff = onOff;
    }

    public String getWarnType() {
        return warnType;
    }

    public void setWarnType(String warnType) {
        this.warnType = warnType;
    }

    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public String getCusID() {
        return cusID;
    }

    public void setCusID(String cusID) {
        this.cusID = cusID;
    }

    public String getCardID() {
        return cardID;
    }

    public void setCardID(String cardID) {
        this.cardID = cardID;
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

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getSpeed() {
        return speed;
    }

    public void setSpeed(String speed) {
        this.speed = speed;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getHeight() {
        return height;
    }

    public void setHeight(String height) {
        this.height = height;
    }

    public String getStatusBits() {
        return statusBits;
    }

    public void setStatusBits(String statusBits) {
        this.statusBits = statusBits;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getGsm() {
        return gsm;
    }

    public void setGsm(String gsm) {
        this.gsm = gsm;
    }

    public String getGps() {
        return gps;
    }

    public void setGps(String gps) {
        this.gps = gps;
    }

    public String getRunTime() {
        return runTime;
    }

    public void setRunTime(String runTime) {
        this.runTime = runTime;
    }

    public String getRunSpeed() {
        return runSpeed;
    }

    public void setRunSpeed(String runSpeed) {
        this.runSpeed = runSpeed;
    }

    public String getPower() {
        return power;
    }

    public void setPower(String power) {
        this.power = power;
    }

    public String getFuel() {
        return fuel;
    }

    public void setFuel(String fuel) {
        this.fuel = fuel;
    }

    public String getMalfunction() {
        return malfunction;
    }

    public void setMalfunction(String malfunction) {
        this.malfunction = malfunction;
    }

    public String getTotalFuel() {
        return totalFuel;
    }

    public void setTotalFuel(String totalFuel) {
        this.totalFuel = totalFuel;
    }

    public String getRunMile() {
        return runMile;
    }

    public void setRunMile(String runMile) {
        this.runMile = runMile;
    }

    public String getTotalMile() {
        return totalMile;
    }

    public void setTotalMile(String totalMile) {
        this.totalMile = totalMile;
    }
}
