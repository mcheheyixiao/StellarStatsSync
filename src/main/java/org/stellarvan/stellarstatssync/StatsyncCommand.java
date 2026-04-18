package org.stellarvan.stellarstatssync;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;

public class StatsyncCommand implements CommandExecutor {

    private final StellarStatsSync plugin;
    private final SyncTask syncTask;

    public StatsyncCommand(StellarStatsSync plugin, SyncTask syncTask) {
        this.plugin = plugin;
        this.syncTask = syncTask;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!sender.hasPermission("stellarstats.admin")) {
            sender.sendMessage("你没有权限使用该命令。");
            return true;
        }

        if (args.length < 1) {
            sender.sendMessage("用法: /" + label + " sync");
            return true;
        }

        if (!args[0].equalsIgnoreCase("sync")) {
            sender.sendMessage("用法: /" + label + " sync");
            return true;
        }

        sender.sendMessage("开始同步玩家数据...");

        // 采集玩家统计必须在主线程；SQL 写入会由 DatabaseManager 异步执行
        syncTask.performSync().whenComplete((result, throwable) -> Bukkit.getScheduler().runTask(plugin, () -> {
            if (throwable != null) {
                sender.sendMessage("同步失败: " + throwable.getMessage());
                if (StellarStatsSync.isDebug()) {
                    plugin.getLogger().severe("[Debug] 手动同步失败: " + throwable.getMessage());
                    throwable.printStackTrace();
                }
                return;
            }

            sender.sendMessage("同步完成: online=" + result.onlinePlayers()
                    + ", snapshots=" + result.snapshots()
                    + ", matchedRegistered=" + result.matchedUsers()
                    + ", durationMs=" + result.durationMs());
        }));

        return true;
    }
}

