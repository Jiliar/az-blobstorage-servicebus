package co.com.jsolutions.functions;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.*;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.*;
import org.springframework.scheduling.annotation.Async;

import java.nio.charset.StandardCharsets;
import java.time.Duration;


/**
 * Azure Functions with HTTP Trigger.
 */

public class BlobTriggerFunction {

    private String connectionString = "<<CONNECTION STRING SERVICE BUS>>";
    private String queueName = "<<QUEUE NAME>>";

    @FunctionName("AzBStoAzSB")
    public void run(
            @BlobTrigger(name = "file",
                    dataType = "binary",
                    path = "adftutorial/{name}.json",
                    connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            final ExecutionContext context
    ){
        context.getLogger().info("--------------------------------------------------------------------");
        context.getLogger().info("INFORMATION ABOUT FILE IN BLOB STORAGE:");
        context.getLogger().info("--------------------------------------------------------------------");
        context.getLogger().info("Name: " + filename + " Size: " + content.length + " bytes");
        context.getLogger().info("--------------------------------------------------------------------");
        //sendServiceBus(context, content);
        receiveServiceBus(context);
    }

    @Async
    public void sendServiceBus(ExecutionContext context, byte[] content){

        try {
            ServiceBusSenderClient sender = new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .sender()
                    .queueName(queueName)
                    .buildClient();
            sender.sendMessage(new ServiceBusMessage(BinaryData.fromBytes(content)));
            sender.close();
        }catch(Exception e){
            context.getLogger().info(e.getMessage());
        }

    }

    @Async
    public void receiveServiceBus(ExecutionContext context){
        try {
            ServiceBusReceiverClient receiver = new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .receiver()
                    .queueName(queueName)
                    .buildClient();

            for (ServiceBusReceivedMessage receiveMessage : receiver.receiveMessages(20, Duration.ofMinutes(1))) {
                context.getLogger().info("--------------------------------------------------------------------");
                context.getLogger().info(receiveMessage.getContentType());
                context.getLogger().info(new String(receiveMessage.getBody().toBytes(), StandardCharsets.UTF_8));
                context.getLogger().info("--------------------------------------------------------------------");
            }
            receiver.close();
        }catch(Exception e){
            context.getLogger().info(e.getMessage());
        }
    }

}
