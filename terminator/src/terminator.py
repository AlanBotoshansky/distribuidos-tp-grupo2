import docker
import random

DONT_KILL_CONTAINERS = ["rabbitmq", "client"]

class Terminator:
    def __init__(self, project_name):
        """
        Initialize the Container Terminator
        
        Args:
            project_name: Docker Compose project name to filter containers
        """
        self.project_name = project_name
        self._docker_client = docker.from_env()
    
    def _is_protected_container(self, container):
        """
        Check if a container is protected from being killed
        
        Args:
            container: Container object to check
            
        Returns:
            True if container should not be killed, False otherwise
        """
        for protected_prefix in DONT_KILL_CONTAINERS:
            if container.name.startswith(protected_prefix):
                return True
        
        if container.name.startswith("health_guard"):
            health_guard_containers = [c for c in self._get_project_containers() if c.name.startswith("health_guard")]
            running_health_guards = [c for c in health_guard_containers if c.status == "running"]
            
            if len(running_health_guards) <= 1:
                return True
        
        return False
    
    def _get_project_containers(self):
        """
        Get all containers belonging to the docker-compose project
        
        Returns:
            List of container objects
        """
        list_filters = {
            "label": f"com.docker.compose.project={self.project_name}",
        }
        containers = self._docker_client.containers.list(all=True, filters=list_filters)
        return containers
    
    def _get_protected_and_killable_containers(self):
        """
        Get lists of protected and killable containers for the project
        """
        containers = self._get_project_containers()
        
        if not containers:
            print(f"No containers found for project '{self.project_name}'")
            return
        
        killable_containers = []
        protected_containers = []
        
        for container in containers:
            if self._is_protected_container(container):
                protected_containers.append(container)
            else:
                killable_containers.append(container)

        return protected_containers, killable_containers
    
    def _kill_container(self, container):
        """
        Kill a specific container
        
        Args:
            container: Docker container object
        """
        if self._is_protected_container(container):
            print(f"[Protected] {container.name}")
            return False
        
        try:
            container.kill()
            print(f"[Killed] {container.name}")
            return True
        except docker.errors.APIError as e:
            print(f"[Failed kill] {container.name}: {e}")
            return False
        
    def kill_containers(self, containers):
        """
        Kill a list of containers
        
        Args:
            containers: List of container objects to kill
        """
        
        killed_count = 0
        for container in containers:
            if self._kill_container(container):
                killed_count += 1
        
        return killed_count
    
    def kill_specific_services(self, service_names):
        """
        Kill containers for specific services
        
        Args:
            service_names: Set of service names to kill
        """
        protected_containers, killable_containers = self._get_protected_and_killable_containers()
        
        print(f"Looking for services: {', '.join(service_names)}")
        if protected_containers:
            print(f"Protected containers (won't be killed): {', '.join([c.name for c in protected_containers])}")
        
        specific_containers = [c for c in killable_containers if c.name in service_names]
        killed_count = self.kill_containers(specific_containers)
        
        print(f"\nSummary: {killed_count} containers killed")
    
    def kill_count(self, count):
        """
        Kill a specific number of containers (randomly selected)
        
        Args:
            count: Number of containers to kill
        """
        protected_containers, killable_containers = self._get_protected_and_killable_containers()
        
        if protected_containers:
            print(f"Protected containers (won't be killed): {', '.join([c.name for c in protected_containers])}")
        
        if count > len(killable_containers):
            print(f"Warning: Requested to kill {count} containers, but only {len(killable_containers)} killable containers available")
            count = len(killable_containers)
        
        if count == 0:
            print("No killable containers available")
            return
        
        selected_containers = random.sample(killable_containers, count)
        print(f"Randomly killing {count} containers from project '{self.project_name}':")
        killed_count = self.kill_containers(selected_containers)
        
        print(f"\nSummary: {killed_count} containers killed")
    
    def kill_all(self):
        """
        Kill all containers belonging to the project
        """
        protected_containers, killable_containers = self._get_protected_and_killable_containers()
        
        if protected_containers:
            print(f"Protected containers (won't be killed): {', '.join([c.name for c in protected_containers])}")
        
        if not killable_containers:
            print("No killable containers available - all containers are protected")
            return
        
        print(f"Killing {len(killable_containers)} containers from project '{self.project_name}':")
        killed_count = self.kill_containers(killable_containers)
        
        print(f"\nSummary: {killed_count} containers killed")
