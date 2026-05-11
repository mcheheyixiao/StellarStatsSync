package org.stellarvan.stellarstatssync.reward;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.ArrayList;
import java.util.List;

public final class RewardPayload {

    private Mail mail;
    private List<Item> items;
    private List<String> commands;
    private Meta meta;

    public static RewardPayload parse(Gson gson, String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("reward_payload_json is blank.");
        }
        try {
            RewardPayload payload = gson.fromJson(json, RewardPayload.class);
            if (payload == null) {
                throw new IllegalArgumentException("reward_payload_json is empty.");
            }
            payload.normalize();
            return payload;
        } catch (JsonSyntaxException ex) {
            throw new IllegalArgumentException("Invalid reward_payload_json: " + ex.getMessage(), ex);
        }
    }

    private void normalize() {
        if (mail == null) {
            mail = new Mail();
        }
        mail.normalize();
        if (items == null) {
            items = List.of();
        } else {
            List<Item> normalized = new ArrayList<>(items.size());
            for (Item item : items) {
                if (item != null) {
                    item.normalize();
                    normalized.add(item);
                }
            }
            items = List.copyOf(normalized);
        }
        if (commands == null) {
            commands = List.of();
        } else {
            List<String> normalized = new ArrayList<>(commands.size());
            for (String command : commands) {
                if (command != null && !command.isBlank()) {
                    normalized.add(command);
                }
            }
            commands = List.copyOf(normalized);
        }
        if (meta == null) {
            meta = new Meta();
        }
        meta.normalize();
    }

    public Mail mail() {
        return mail;
    }

    public List<Item> items() {
        return items;
    }

    public List<String> commands() {
        return commands;
    }

    public Meta meta() {
        return meta;
    }

    public boolean hasMail() {
        return !items.isEmpty()
                || mail.hasTitle()
                || mail.hasIcon()
                || !mail.content().isEmpty();
    }

    public boolean hasCommands() {
        return !commands.isEmpty();
    }

    public boolean isEmpty() {
        return !hasMail() && !hasCommands();
    }

    public static final class Mail {
        private String title;
        private String icon;
        private List<String> content;

        private void normalize() {
            title = title == null ? "" : title.trim();
            icon = icon == null ? "" : icon.trim();
            if (content == null) {
                content = List.of();
                return;
            }
            List<String> normalized = new ArrayList<>(content.size());
            for (String line : content) {
                if (line != null) {
                    normalized.add(line);
                }
            }
            content = List.copyOf(normalized);
        }

        public String title() {
            return title;
        }

        public String icon() {
            return icon;
        }

        public List<String> content() {
            return content;
        }

        public boolean hasTitle() {
            return title != null && !title.isBlank();
        }

        public boolean hasIcon() {
            return icon != null && !icon.isBlank();
        }
    }

    public static final class Item {
        private String type;
        private int amount = 1;

        private void normalize() {
            type = type == null ? "" : type.trim();
            if (amount <= 0) {
                amount = 1;
            }
        }

        public String type() {
            return type;
        }

        public int amount() {
            return amount;
        }
    }

    public static final class Meta {
        private String signDate;
        private int continuous;
        private int total;
        private String source;

        private void normalize() {
            signDate = signDate == null ? "" : signDate.trim();
            source = source == null ? "" : source.trim();
        }

        public String signDate() {
            return signDate;
        }

        public int continuous() {
            return continuous;
        }

        public int total() {
            return total;
        }

        public String source() {
            return source;
        }
    }
}
