package com.quickbus.busbooking.service;

import com.quickbus.busbooking.config.KafkaConfig;
import com.quickbus.busbooking.dto.EmailEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class EmailConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(EmailConsumerService.class);

    @Autowired
    private JavaMailSender mailSender;

    @KafkaListener(
            topics = KafkaConfig.EMAIL_TOPIC,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEmailEvent(
            @Payload EmailEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        logger.info("Received email event: Type={}, BookingId={}, Partition={}, Offset={}",
                event.getEmailType(), event.getBookingId(), partition, offset);

        try {
            switch (event.getEmailType()) {
                case BOOKING_CONFIRMATION:
                    sendBookingConfirmationEmail(event);
                    break;
                case FULL_CANCELLATION:
                    sendFullCancellationEmail(event);
                    break;
                case PARTIAL_CANCELLATION:
                    sendPartialCancellationEmail(event);
                    break;
                default:
                    logger.warn("Unknown email type: {}", event.getEmailType());
            }

            logger.info("Email sent successfully: Type={}, BookingId={}, Email={}",
                    event.getEmailType(), event.getBookingId(), event.getUserEmail());

        } catch (Exception e) {
            logger.error("Failed to send email: Type={}, BookingId={}, Error={}",
                    event.getEmailType(), event.getBookingId(), e.getMessage(), e);
            // In production, you might want to send to DLQ or retry
            throw new RuntimeException("Email sending failed", e);
        }
    }

    private void sendBookingConfirmationEmail(EmailEvent event) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(event.getUserEmail());
        message.setSubject("Booking Confirmation - " + event.getBusName());

        String text = "Hello " + event.getUserName() + ",\n"
                + "Your booking is confirmed!\n"
                + "Booking ID: " + event.getBookingId() + "\n"
                + "Seats Booked: " + event.getSeatsBooked() + "\n"
                + "Total Fare: ₹" + event.getTotalFare() + "\n"
                + "Travel Date: " + event.getTravelDate() + "\n"
                + "Departure: " + event.getDepartureTime() + "\n"
                + "Arrival: " + event.getArrivalTime() + "\n"
                + "From: " + event.getSource() + "\n"
                + "To: " + event.getDestination() + "\n"
                + "Bus: " + event.getBusName() + " (" + event.getBusType() + ")\n"
                + "Thank you for using QuickBus!";

        message.setText(text);
        mailSender.send(message);
    }

    private void sendFullCancellationEmail(EmailEvent event) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(event.getUserEmail());
        message.setSubject("Booking Cancelled - QuickBus");

        String text = "Hello " + event.getUserName() + ",\n"
                + "Your booking has been cancelled successfully.\n"
                + "Booking ID: " + event.getBookingId() + "\n"
                + "Cancelled Seats: " + event.getSeatsBooked() + "\n"
                + "Refund Amount (if applicable): ₹" + event.getTotalFare() + "\n"
                + "We hope to see you again soon!\n"
                + "Regards,\nQuickBus Team";

        message.setText(text);
        mailSender.send(message);
    }

    private void sendPartialCancellationEmail(EmailEvent event) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(event.getUserEmail());
        message.setSubject("Booking Cancelled - QuickBus");

        String text = "Hello " + event.getUserName() + ",\n"
                + "Your cancellation was successful.\n"
                + "Booking ID: " + event.getBookingId() + "\n"
                + "Cancelled Seats: " + event.getSeatsCancelled() + "\n"
                + "Remaining Seats: " + event.getSeatsBooked() + "\n"
                + "Refund Amount: ₹" + event.getRefundAmount() + "\n"
                + "Thank you for choosing QuickBus.";

        message.setText(text);
        mailSender.send(message);
    }
}