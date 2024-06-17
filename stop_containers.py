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
command = 'docker-compose -f "C:\\Users\\Vikash Singh\\repos\\liveangel\\docker-compose-tfetch.yml" down'
stdout, stderr = execute_powershell_command(command)

if stderr:
    print(f"Error in stopping Tick Fetcher: {str(stderr)}")
else:
    print(f"Tick Fetcher stop response: {str(stdout)}")

# Run candle maker
command = 'docker-compose -f "C:\\Users\\Vikash Singh\\repos\\liveangel\\docker-compose-cmaker.yml" down'
stdout, stderr = execute_powershell_command(command)

if stderr:
    print(f"Error in stopping Candle Maker: {str(stderr)}")
else:
    print(f"Candle Maker stop response: {str(stdout)}")

# Run strength meter
command = 'docker-compose -f "C:\\Users\Vikash Singh\\repos\\arahant\\docker-compose-1min.yml" down'
stdout, stderr = execute_powershell_command(command)

if stderr:
    print(f"Error in stopping Strength Meter: {str(stderr)}")
else:
    print(f"Strength Meter stop response: {str(stdout)}")