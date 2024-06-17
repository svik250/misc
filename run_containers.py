import subprocess

def execute_powershell_command(command):
    try:
        # Run the PowerShell command
        completed_process = subprocess.run(
            ["powershell", "-Command", command],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Return the stdout and stderr
        return completed_process.stdout, completed_process.stderr
    except subprocess.CalledProcessError as e:
        # Handle errors in command execution
        return e.stdout, e.stderr

# Run tick fetcher
command = 'docker-compose -f "C:\\Users\\Vikash Singh\\repos\\liveangel\\docker-compose-tfetch.yml" up -d'
stdout, stderr = execute_powershell_command(command)

if stderr:
    print(f"Error in running Tick Fetcher: {str(stderr)}")
else:
    print(f"Tick Fetcher run response: {str(stdout)}")

# Run candle maker
command = 'docker-compose -f "C:\\Users\\Vikash Singh\\repos\\liveangel\\docker-compose-cmaker.yml" up -d'
stdout, stderr = execute_powershell_command(command)

if stderr:
    print(f"Error in running Candle Maker: {str(stderr)}")
else:
    print(f"Candle Maker run response: {str(stdout)}")

# Run strength meter
command = 'docker-compose -f "C:\\Users\Vikash Singh\\repos\\arahant\\docker-compose-1min.yml" up -d'
stdout, stderr = execute_powershell_command(command)

if stderr:
    print(f"Error in running Strength Meter: {str(stderr)}")
else:
    print(f"Strength Meter run response: {str(stdout)}")