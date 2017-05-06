package com.architecture.spark.model;

public class HearBeat {

    private String beat;

    private String identificationNumber;

    private String date;


    public HearBeat(String beat, String identificationNumber, String date) {
        this.beat = beat;
        this.identificationNumber = identificationNumber;
        this.date = date;
    }

    @Override
    public String toString() {
        return "{\"beat\":" +"\"" + beat  +"\"" + ", \"identificationNumber\":"  +"\"" + identificationNumber  +"\"" +  ", \"date\":" +"\"" + date +"\"" + "}";
    }


}
