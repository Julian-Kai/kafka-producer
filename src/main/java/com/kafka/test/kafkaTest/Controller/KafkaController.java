package com.kafka.test.kafkaTest.Controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.test.kafkaTest.Service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

    private final ProducerService producer;
    private String returnMessageNotification = "";
    private String returnMessageObject = "";

    private Date dNow = new Date();
    private SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd hh:mm");

    @Autowired
    KafkaController(ProducerService producer) {
        this.producer = producer;
    }

    @PostMapping(value = "/publish/notificationService/{round}/{batch}")
    @ResponseBody
    public String sendMessageToKafkaTopic(@RequestBody JSONObject json, @PathVariable int round, @PathVariable int batch) throws InterruptedException {
        // FIXME
        String key = "";
        String topic = "myTopic";

        ListenableFuture<SendResult<String, String>> future = null;
        for (int i = 1; i <= batch; i++) {
            future = this.producer.sendMessage(topic, key + UUID.randomUUID(), json.toString());
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    String msg = "***** Kafka callback success! *****";
                    returnMessageNotification = msg + "\n" + result.toString();
                    System.out.println(result.toString());
                }

                @Override
                public void onFailure(Throwable ex) {
                    String msg = "***** Kafka callback fail! *****";
                    returnMessageNotification = msg + "\n" + ex.toString();
                    System.out.println(ex.toString());
                }
            });
        }
        return returnMessageNotification;
    }

    @PostMapping(value = "/publish/objectService/{round}/{batch}")
    @ResponseBody
    public String sendMessageToKafkaTopic(@RequestBody JSONArray json, @PathVariable int round, @PathVariable int batch) throws InterruptedException {
        // FIXME
        String key = "";
        String topic = "myTopic";

        ListenableFuture<SendResult<String, String>> future = null;
        for (int i = 1; i <= batch; i++) {
            future = this.producer.sendMessage(topic, key + i + "_" + UUID.randomUUID(), json.toString());
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    String msg = "***** Kafka callback success! *****";
                    returnMessageObject = msg + "\n" + result.toString();
                    System.out.println(result.toString());
                }

                @Override
                public void onFailure(Throwable ex) {
                    String msg = "***** Kafka callback fail! *****";
                    returnMessageObject = msg + "\n" + ex.toString();
                    System.out.println(ex.toString());
                }
            });
        }
        return returnMessageObject;
    }
}