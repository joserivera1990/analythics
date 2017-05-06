package com.architecture.spark.model;

import java.util.List;

public class HearBeatMongo {

        private List<String> _id;

        private String steps;

        private String identificationNumber;

        private String date;

        public void set_id(List<String> _id) {
            this._id = _id;
        }

        public void setSteps(String steps) {
            this.steps = steps;
        }

        public void setIdentificationNumber(String identificationNumber) {
            this.identificationNumber = identificationNumber;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public List<String> get_id() {
            return _id;
        }

        public String getSteps() {
            return steps;
        }

        public String getIdentificationNumber() {
            return identificationNumber;
        }

        public String getDate() {
            return date;
        }







}
