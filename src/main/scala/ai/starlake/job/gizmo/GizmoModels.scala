package ai.starlake.job.gizmo

/** Request to start a new process
  * @param processName:
  *   name of the process
  * @param connectionName:
  *   connection name
  * @param port:
  *   optional port
  * @param arguments:
  *   map of arguments. Expected arguments:
  *   - GIZMOSQL_USERNAME
  *   - GIZMOSQL_PASSWORD
  *   - SL_DB_ID
  *   - SL_DATA_PATH
  *   - PG_USERNAME
  *   - PG_PASSWORD
  *   - PG_PORT
  *   - PG_HOST
  */
case class StartProcessRequest(
  processName: String,
  connectionName: String,
  port: Option[Int] = None,
  arguments: Map[String, String] = Map.empty
)

case class StartProcessResponse(
  processName: String,
  connectionName: String,
  port: Int,
  message: String
)

case class StopProcessRequest(processName: String)

case class StopProcessResponse(processName: String, message: String)

case class RestartProcessRequest(processName: String)

case class RestartProcessResponse(processName: String, port: Int, message: String)

case class ProcessInfo(processName: String, port: Int, pid: Option[Long], status: String)

case class ListProcessesResponse(processes: List[ProcessInfo])

case class GizmoErrorResponse(error: String)
