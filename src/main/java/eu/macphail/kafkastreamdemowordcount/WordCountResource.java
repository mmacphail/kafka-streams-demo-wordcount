package eu.macphail.kafkastreamdemowordcount;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class WordCountResource {

    private static final Logger log = LoggerFactory.getLogger(WordCountResource.class);

    private ReadOnlyKeyValueStore<String, Long> keyValueStore;

    @GetMapping("words/{name}")
    public Long wordCount(@PathVariable("name") String name) {
        Optional<Long> count = Optional.ofNullable(keyValueStore.get(name));
        return count.orElse(0L);
    }

    public void setKeyValueStore(ReadOnlyKeyValueStore<String, Long> keyValueStore) {
        this.keyValueStore = keyValueStore;
    }
}
