package org.mora.cep.reader;

import javax.jms.*;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.mora.cep.cepProcessing.Position;
import org.mora.cep.cepProcessing.TaskRepeat;

import java.util.LinkedList;

import java.util.*;

/**
 * Created by ruveni on 30/10/15.
 */
public class Consumer extends TimerTask {
    // URL of the JMS server
    private static String url = "tcp://localhost:61616";
    // Name of the queue we will receive messages from
    private static String subject = "queueMap";
    MessageConsumer consumer = null;
    Connection connection = null;
    Session session = null;
    Timer timer=null;
    LinkedList<Position> level2Queue;

    public Consumer(Timer timer, LinkedList<Position> l2Q) {
        this.timer=timer;
        this.level2Queue = l2Q;

        try {
            BasicConfigurator.configure();
            // Getting JMS connection from the server
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
            connection = connectionFactory.createConnection();
            connection.start();
            // Creating session for sending messages
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);

            // Getting the queue
            Destination destination = session.createQueue(subject);
            // MessageConsumer is used for receiving (consuming) messages

            consumer = session.createConsumer(destination);
            // Here we receive the message.

            // By default this call is blocking, which means it will wait

            // for a message to arrive on the queue.
        } catch (Exception e) {
            e.printStackTrace();
            timer.cancel();
        }
    }

    @Override
    public void run() {
        try {
            Message message = consumer.receive();
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String msg = textMessage.getText();

                level2Queue.add(getPosition(msg));
                System.out.println("Received message '" + textMessage.getText() + "'");
                System.out.println();
            }
        } catch (Exception e) {
            timer.cancel();
            e.printStackTrace();
            try{
                connection.close();
            }catch (Exception ex){
                //ex.printStackTrace();
            }
        }
    }

    public Position getPosition(String recievedmsg){
//        int idxa = recievedmsg.indexOf("latitude:") + 9;
//        int idxb = recievedmsg.indexOf("longitude:") + 10;
//
//        String lat = recievedmsg.substring(idxa,4);
//        String lon = recievedmsg.substring(idxb,4);
//
//        return new Position(Double.parseDouble(lat), Double.parseDouble(lon));
        String[] arr=recievedmsg.split(",");
        String[] latAr=arr[3].split(":");
        String[] lonAr=arr[4].split(":");
        System.out.println(level2Queue.size());
        return new Position(Double.parseDouble(latAr[1]),Double.parseDouble(lonAr[1]));
    }
}