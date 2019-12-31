package eu.macphail.kafkastreamdemowordcount;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WordCountResource {

    private static final Logger log = LoggerFactory.getLogger(WordCountResource.class);

    @Autowired
    private ApplicationContext context;

    private ReadOnlyKeyValueStore<String, Long> keyValueStore;

    @GetMapping("words/{name}")
    public String wordCount(@PathVariable("name") String name) {
        try {
            return keyValueStore.get(name).toString();
        } catch (NullPointerException e) {
            return "0";
        }
    }

    public void setKeyValueStore(ReadOnlyKeyValueStore<String, Long> keyValueStore) {
        this.keyValueStore = keyValueStore;
    }
}
