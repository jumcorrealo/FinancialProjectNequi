import unittest
from unittest.mock import patch,MagicMock
import connection as connection




class TestConnection(unittest.TestCase):

    @patch('connection.configparser.ConfigParser')
    @patch('connection.psycopg2.connect')
    def test_connect(self, mock_connect, mock_config):
        

        mock_connect.return_value = 'Mocked Connection'

        mock_config.return_value = {'database': {'host': connection.db_endpoint_RDS, 'database_name': connection.db_name_RDS,
                                                 'username': connection.db_user_RDS, 'password': connection.db_password_RDS, 'port': connection.db_port_RDS},
                                    'redshift': {'host': connection.db_endpoint_RSH, 'database_name': connection.db_name_RSH,
                                                 'username': connection.db_user_RSH, 'password': connection.db_password_RSH, 'port': connection.db_port_RSH}}

        # Call the function to connect to databases
        conn_rds = connection.connect_to_RDS()
        conn_rsh = connection.connection_to_RedShift()


        # Assert that the connections are correctly established
        self.assertEqual(conn_rds, 'Mocked Connection')
        self.assertEqual(conn_rsh, 'Mocked Connection')

        # Assert that the correct parameters are passed to psycopg2.connect
        mock_connect.assert_any_call(host=connection.db_endpoint_RDS, port=connection.db_port_RDS, database=connection.db_name_RDS,
                                     user=connection.db_user_RDS, password=connection.db_password_RDS)

        mock_connect.assert_any_call(host=connection.db_endpoint_RSH, port=connection.db_port_RSH, database=connection.db_name_RSH,
                                     user=connection.db_user_RSH, password=connection.db_password_RSH)
        

    def test_closeconnection(self):

        mock_conn_rds = MagicMock()
        mock_conn_rsh = MagicMock()

        mock_conn_rds.close.return_value = None
        mock_conn_rsh.close.return_value = None

        connection.close_connections(mock_conn_rds)
        connection.close_connections(mock_conn_rsh)

        mock_conn_rsh.close.assert_called_once()
        mock_conn_rds.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
