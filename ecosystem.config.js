module.exports = {
    apps: [{
        name: "orchestrator",
        script: "./run_safe.sh",
        interpreter: "bash",
        cron_restart: "*/10 * * * *",
        autorestart: false,
        watch: false,
        env: {
            NODE_ENV: "production",
        }
    }]
};
