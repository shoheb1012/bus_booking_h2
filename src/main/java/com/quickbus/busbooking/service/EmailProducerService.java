package com.quickbus.busbooking.service;

import com.quickbus.busbooking.config.KafkaConfig;
import com.quickbus.busbooking.dto.EmailEvent;
import com.quickbus.busbooking.entity.Booking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class EmailProducerService {

    private static final Logger logger = LoggerFactory.getLogger(EmailProducerService.class);

    @Autowired
    private KafkaTemplate<String, EmailEvent> kafkaTemplate;

    public void sendBookingConfirmationEvent(Booking booking) {
        EmailEvent event = createEmailEvent(booking, EmailEvent.EmailType.BOOKING_CONFIRMATION);
        sendEvent(event);
    }

    public void sendFullCancellationEvent(Booking booking) {
        EmailEvent event = createEmailEvent(booking, EmailEvent.EmailType.FULL_CANCELLATION);
        sendEvent(event);
    }

    public void sendPartialCancellationEvent(Booking booking, int seatsCancelled, double refundAmount) {
        EmailEvent event = createEmailEvent(booking, EmailEvent.EmailType.PARTIAL_CANCELLATION);
        event.setSeatsCancelled(seatsCancelled);
        event.setRefundAmount(refundAmount);
        sendEvent(event);
    }

    private EmailEvent createEmailEvent(Booking booking, EmailEvent.EmailType emailType) {
        EmailEvent event = new EmailEvent();
        event.setEmailType(emailType);
        event.setUserEmail(booking.getUser().getEmailId());
        event.setUserName(booking.getUser().getName());
        event.setBookingId(booking.getId());
        event.setSeatsBooked(booking.getSeatsBooked());
        event.setTotalFare(booking.getTotalFare());
        event.setTravelDate(booking.getSchedule().getTravelDate());
        event.setDepartureTime(booking.getSchedule().getDepartureTime());
        event.setArrivalTime(booking.getSchedule().getArrivalTime());
        event.setSource(booking.getSchedule().getRoute().getSource());
        event.setDestination(booking.getSchedule().getRoute().getDestination());
        event.setBusName(booking.getSchedule().getBus().getBusName());
        event.setBusType(booking.getSchedule().getBus().getBusType());
        return event;
    }

    private void sendEvent(EmailEvent event) {
        try {
            // Using booking ID as key for partitioning
            String key = event.getBookingId().toString();

            CompletableFuture<SendResult<String, EmailEvent>> future =
                    kafkaTemplate.send(KafkaConfig.EMAIL_TOPIC, key, event);

            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    logger.info("Email event sent successfully: Type={}, BookingId={}, Partition={}, Offset={}",
                            event.getEmailType(),
                            event.getBookingId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                } else {
                    logger.error("Failed to send email event: Type={}, BookingId={}, Error={}",
                            event.getEmailType(),
                            event.getBookingId(),
                            ex.getMessage());
                }
            });

        } catch (Exception e) {
            logger.error("Exception while sending email event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to send email event", e);
        }
    }
}