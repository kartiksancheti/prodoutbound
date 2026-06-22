module.exports = {
  apps: [
    {
      name: "outbound-server",
      script: "python3",
      args: "-m uvicorn server:app --host 0.0.0.0 --port 8000",
      cwd: "/home/prodoutbound",
      interpreter: "none",
      env_file: "/home/prodoutbound/.env",
      restart_delay: 3000,
      max_restarts: 10,
      autorestart: true,
    },
    {
      name: "outbound-agent",
      script: "/home/prodoutbound/start_agent.sh",
      args: "",
      cwd: "/home/prodoutbound",
      interpreter: "none",
      env_file: "/home/prodoutbound/.env",
      instances: 1,
      restart_delay: 5000,
      max_restarts: 10,
      autorestart: true,
    }
  ]
}
