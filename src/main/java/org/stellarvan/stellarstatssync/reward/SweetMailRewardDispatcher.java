package org.stellarvan.stellarstatssync.reward;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;
import org.stellarvan.stellarstatssync.StellarStatsSync;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class SweetMailRewardDispatcher {

    private static final String PLUGIN_NAME = "SweetMail";
    private static final String API_CLASS_NAME = "top.mrxiaom.sweetmail.IMail";
    private static final String ATTACHMENT_ITEM_CLASS_NAME = "top.mrxiaom.sweetmail.attachments.AttachmentItem";

    private final StellarStatsSync plugin;
    private final RewardOutboxSettings.SweetMailSettings settings;

    private volatile ReflectionAccess reflectionAccess;
    private volatile Plugin cachedProvider;

    public SweetMailRewardDispatcher(StellarStatsSync plugin, RewardOutboxSettings.SweetMailSettings settings) {
        this.plugin = plugin;
        this.settings = settings;
    }

    public boolean isConfiguredEnabled() {
        return settings.enabled();
    }

    public boolean isPluginInstalled() {
        return plugin.getServer().getPluginManager().getPlugin(PLUGIN_NAME) != null;
    }

    public boolean isPluginEnabled() {
        Plugin provider = plugin.getServer().getPluginManager().getPlugin(PLUGIN_NAME);
        return provider != null && provider.isEnabled();
    }

    public void sendSystemMail(
            UUID playerUuid,
            String senderDisplay,
            String icon,
            String title,
            List<String> content,
            List<ItemStack> attachments
    ) throws RewardDispatchException {
        if (!settings.enabled()) {
            throw new RewardDispatchException("SweetMail dispatch is disabled by reward-outbox.sweetmail.enabled.", true, false);
        }

        Plugin provider = plugin.getServer().getPluginManager().getPlugin(PLUGIN_NAME);
        if (provider == null) {
            String message = settings.requirePlugin()
                    ? "SweetMail plugin is not installed."
                    : "SweetMail plugin is unavailable.";
            throw new RewardDispatchException(message, true, false);
        }
        if (!provider.isEnabled()) {
            throw new RewardDispatchException("SweetMail plugin is installed but not enabled.", true, false);
        }

        ReflectionAccess access = resolveAccess(provider);
        OfflinePlayer receiver = Bukkit.getOfflinePlayer(playerUuid);
        try {
            Object api = access.apiMethod.invoke(null);
            Object draft = access.createSystemMailMethod.invoke(api, senderDisplay);
            access.setIconMethod.invoke(draft, icon);
            access.setTitleMethod.invoke(draft, title);
            access.setContentMethod.invoke(draft, content);
            access.setReceiverMethod.invoke(draft, receiver);

            if (attachments != null && !attachments.isEmpty()) {
                List<Object> mailAttachments = new ArrayList<>(attachments.size());
                for (ItemStack attachment : attachments) {
                    mailAttachments.add(access.buildAttachmentMethod.invoke(null, attachment));
                }
                access.addAttachmentsMethod.invoke(draft, mailAttachments);
            }

            Object status = access.sendMethod.invoke(draft);
            boolean ok = Boolean.TRUE.equals(access.statusOkMethod.invoke(status));
            if (!ok) {
                throw new RewardDispatchException("SweetMail rejected the system mail for receiver " + playerUuid + ": " + status, true, false);
            }
        } catch (RewardDispatchException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RewardDispatchException("SweetMail API invocation failed: " + summarize(ex), true, false, ex);
        }
    }

    private ReflectionAccess resolveAccess(Plugin provider) throws RewardDispatchException {
        ReflectionAccess cached = reflectionAccess;
        if (cached != null && cachedProvider == provider) {
            return cached;
        }

        try {
            ClassLoader classLoader = provider.getClass().getClassLoader();
            Class<?> apiClass = Class.forName(API_CLASS_NAME, true, classLoader);
            Class<?> attachmentItemClass = Class.forName(ATTACHMENT_ITEM_CLASS_NAME, true, classLoader);

            Method apiMethod = apiClass.getMethod("api");
            Method createSystemMailMethod = apiClass.getMethod("createSystemMail", String.class);

            Class<?> mailDraftClass = null;
            for (Class<?> innerClass : apiClass.getDeclaredClasses()) {
                if ("MailDraft".equals(innerClass.getSimpleName())) {
                    mailDraftClass = innerClass;
                    break;
                }
            }
            if (mailDraftClass == null) {
                throw new IllegalStateException("IMail.MailDraft class not found.");
            }

            Method setReceiverMethod = mailDraftClass.getMethod("setReceiver", OfflinePlayer.class);
            Method setIconMethod = mailDraftClass.getMethod("setIcon", String.class);
            Method setTitleMethod = mailDraftClass.getMethod("setTitle", String.class);
            Method setContentMethod = mailDraftClass.getMethod("setContent", List.class);
            Method addAttachmentsMethod = mailDraftClass.getMethod("addAttachments", java.util.Collection.class);
            Method sendMethod = mailDraftClass.getMethod("send");
            Method buildAttachmentMethod = attachmentItemClass.getMethod("build", ItemStack.class);
            Method statusOkMethod = sendMethod.getReturnType().getMethod("ok");

            ReflectionAccess resolved = new ReflectionAccess(
                    apiMethod,
                    createSystemMailMethod,
                    setReceiverMethod,
                    setIconMethod,
                    setTitleMethod,
                    setContentMethod,
                    addAttachmentsMethod,
                    buildAttachmentMethod,
                    statusOkMethod,
                    sendMethod
            );
            this.cachedProvider = provider;
            this.reflectionAccess = resolved;
            return resolved;
        } catch (Exception ex) {
            throw new RewardDispatchException("Failed to resolve SweetMail API: " + summarize(ex), true, false, ex);
        }
    }

    private static String summarize(Throwable throwable) {
        Throwable root = throwable;
        while (root != null && root.getCause() != null) {
            root = root.getCause();
        }
        if (root == null) {
            return "unknown error";
        }
        String message = root.getMessage();
        return message == null || message.isBlank() ? root.getClass().getSimpleName() : message.trim();
    }

    private record ReflectionAccess(
            Method apiMethod,
            Method createSystemMailMethod,
            Method setReceiverMethod,
            Method setIconMethod,
            Method setTitleMethod,
            Method setContentMethod,
            Method addAttachmentsMethod,
            Method buildAttachmentMethod,
            Method statusOkMethod,
            Method sendMethod
    ) {
    }
}
