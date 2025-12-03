package com.quickbus.busbooking.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import com.quickbus.busbooking.entity.Booking;

@Service
public class EmailService {

    @Autowired
    private JavaMailSender mailSender;

    @Autowired
    private EmailProducerService emailProducerService;

    @Value("${email.use-kafka:true}")
    private boolean useKafka;

    public void sendBookingConfirmation(Booking booking) {
        if (useKafka) {
            // New way: Send to Kafka
            emailProducerService.sendBookingConfirmationEvent(booking);
        } else {
            // Old way: Direct email sending (fallback)
            sendBookingConfirmationDirect(booking);
        }
    }

    public void sendCancellationMail(Booking booking) {
        if (useKafka) {
            // New way: Send to Kafka
            emailProducerService.sendFullCancellationEvent(booking);
        } else {
            // Old way: Direct email sending (fallback)
            sendFullCancellationDirect(booking);
        }
    }

    public void sendCancellationMail(Booking booking, int seatsCancelled, double refundAmount) {
        if (useKafka) {
            // New way: Send to Kafka
            emailProducerService.sendPartialCancellationEvent(booking, seatsCancelled, refundAmount);
        } else {
            // Old way: Direct email sending (fallback)
            sendPartialCancellationDirect(booking, seatsCancelled, refundAmount);
        }
    }

    // Keep original methods for fallback and consumer to use
    public void sendBookingConfirmationDirect(Booking booking) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(booking.getUser().getEmailId());
        message.setSubject("Booking Confirmation - " + booking.getSchedule().getBus().getBusName());

        String text = "Hello " + booking.getUser().getName() + ",\n"
                + "Your booking is confirmed!\n"
                + "Booking ID: " + booking.getId() + "\n"
                + "Seats Booked: " + booking.getSeatsBooked() + "\n"
                + "Total Fare: ₹" + booking.getTotalFare() + "\n"
                + "Travel Date: " + booking.getSchedule().getTravelDate() + "\n"
                + "Departure: " + booking.getSchedule().getDepartureTime() + "\n"
                + "Arrival: " + booking.getSchedule().getArrivalTime() + "\n"
                + "From: " + booking.getSchedule().getRoute().getSource() + "\n"
                + "To: " + booking.getSchedule().getRoute().getDestination() + "\n"
                + "Bus: " + booking.getSchedule().getBus().getBusName() + " (" + booking.getSchedule().getBus().getBusType() + ")\n"
                + "Thank you for using QuickBus!";

        message.setText(text);
        mailSender.send(message);
    }

    public void sendFullCancellationDirect(Booking booking) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(booking.getUser().getEmailId());
        message.setSubject(" Booking Cancelled - QuickBus");

        String text = "Hello " + booking.getUser().getName() + ",\n"
                + "Your booking has been cancelled successfully.\n"
                + "Booking ID: " + booking.getId() + "\n"
                + "Cancelled Seats: " + booking.getSeatsBooked() + "\n"
                + "Refund Amount (if applicable): ₹" + booking.getTotalFare() + "\n"
                + "We hope to see you again soon!\n"
                + "Regards,\nQuickBus Team";

        message.setText(text);
        mailSender.send(message);
    }

    public void sendPartialCancellationDirect(Booking booking, int seatsCancelled, double refundAmount) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(booking.getUser().getEmailId());
        message.setSubject("Booking Cancelled - QuickBus");

        String text = "Hello " + booking.getUser().getName() + ",\n"
                + "Your cancellation was successful.\n"
                + "Booking ID: " + booking.getId() + "\n"
                + "Cancelled Seats: " + seatsCancelled + "\n"
                + "Remaining Seats: " + (booking.getSeatsBooked()) + "\n"
                + "Refund Amount: ₹" + refundAmount + "\n"
                + "Thank you for choosing QuickBus.";

        message.setText(text);
        mailSender.send(message);
    }
}