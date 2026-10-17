package cl.duoc.vidasalud.notify;

import cl.duoc.vidasalud.notify.config.NombresRabbit;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(NombresRabbit.class)
public class MsVidasaludNotifyApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsVidasaludNotifyApplication.class, args);
    }
}