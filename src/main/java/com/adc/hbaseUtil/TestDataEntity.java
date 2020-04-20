package com.adc.hbaseUtil;

public class TestDataEntity {
    private String mdytime;
    private String hmstime;
    private String mstime;
    private double DynoSpeed;
    private double DynoTorque;
    private double IntakeAirPressG;
    private double CACDP;
    private double ExhDownpipePressG;
    private double ChargeCoolerOutTemp;

    public TestDataEntity() {
    }

    public TestDataEntity(String mdytime, String hmstime, String mstime, double dynoSpeed, double dynoTorque, double intakeAirPressG, double CACDP, double exhDownpipePressG, double chargeCoolerOutTemp) {
        this.mdytime = mdytime;
        this.hmstime = hmstime;
        this.mstime = mstime;
        DynoSpeed = dynoSpeed;
        DynoTorque = dynoTorque;
        IntakeAirPressG = intakeAirPressG;
        this.CACDP = CACDP;
        ExhDownpipePressG = exhDownpipePressG;
        ChargeCoolerOutTemp = chargeCoolerOutTemp;
    }

    public String getMdytime() {
        return mdytime;
    }

    public void setMdytime(String mdytime) {
        this.mdytime = mdytime;
    }

    public String getHmstime() {
        return hmstime;
    }

    public void setHmstime(String hmstime) {
        this.hmstime = hmstime;
    }

    public String getMstime() {
        return mstime;
    }

    public void setMstime(String mstime) {
        this.mstime = mstime;
    }

    public double getDynoSpeed() {
        return DynoSpeed;
    }

    public void setDynoSpeed(double dynoSpeed) {
        DynoSpeed = dynoSpeed;
    }

    public double getDynoTorque() {
        return DynoTorque;
    }

    public void setDynoTorque(double dynoTorque) {
        DynoTorque = dynoTorque;
    }

    public double getIntakeAirPressG() {
        return IntakeAirPressG;
    }

    public void setIntakeAirPressG(double intakeAirPressG) {
        IntakeAirPressG = intakeAirPressG;
    }

    public double getCACDP() {
        return CACDP;
    }

    public void setCACDP(double CACDP) {
        this.CACDP = CACDP;
    }

    public double getExhDownpipePressG() {
        return ExhDownpipePressG;
    }

    public void setExhDownpipePressG(double exhDownpipePressG) {
        ExhDownpipePressG = exhDownpipePressG;
    }

    public double getChargeCoolerOutTemp() {
        return ChargeCoolerOutTemp;
    }

    public void setChargeCoolerOutTemp(double chargeCoolerOutTemp) {
        ChargeCoolerOutTemp = chargeCoolerOutTemp;
    }

    @Override
    public String toString() {
        return "TestDataEntity{" +
                "mdytime='" + mdytime + '\'' +
                ", hmstime='" + hmstime + '\'' +
                ", mstime='" + mstime + '\'' +
                ", DynoSpeed=" + DynoSpeed +
                ", DynoTorque=" + DynoTorque +
                ", IntakeAirPressG=" + IntakeAirPressG +
                ", CACDP=" + CACDP +
                ", ExhDownpipePressG=" + ExhDownpipePressG +
                ", ChargeCoolerOutTemp=" + ChargeCoolerOutTemp +
                '}';
    }
}
