# MySQL

The MySqlRegistry supports persisting all registered TableDescriptors into a
MySQL database table. The following describes the configurations required to set
up a MySqlRegistry instance.

<!-- TODO: document the schema of the mysql table and the behavior when the feature descriptor is updated. -->

## Configurations

| Key      | Required | Default | Type    | Description                                                  |
| -------- | -------- | ------- | ------- | ------------------------------------------------------------ |
| database | Required | -       | String  | The name of the MySQL database to hold the Feathub registry. |
| table    | Required | -       | String  | The name of the MySQL table to hold the Feathub registry.    |
| host     | Required | -       | String  | IP address or hostname of the MySQL server.                  |
| port     | Optional | 3306    | Integer | The port of the MySQL server.                                |
| username | Optional | (None)  | String  | Name of the user to connect to the MySQL server.             |
| password | Optional | (None)  | String  | The password of the user.                                    |

## Examples

Here is an example that creates a FeathubClient that persists TableDescriptors
to MySQL.

```python
client = FeathubClient(
    {
        "processor": {
            "type": "local",
        },
        "online_store": {
            "types": ["memory"],
            "memory": {},
        },
        "registry": {
            "type": "mysql",
            "mysql": {
                "database": "default",
                "table": "feathub_registry_features",
                "host": "127.0.0.1",
                "port": 3306,
                "username": "admin",
                "password": "123456",
            },
        },
        "feature_service": {
            "type": "local",
            "local": {},
        },
    }
)
```
