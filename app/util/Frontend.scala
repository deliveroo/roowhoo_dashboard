package util

import kafka.coordinator.group.ClientDetails

object Frontend {

  val riskClientIds = Seq("rdkafka", "ruby-kafka", "consumer-1")

  def colorRow(clientDetails: ClientDetails): String  = {
    if(riskClientIds.contains(clientDetails.clientId)) "text-light risk"
    else {
      if(clientDetails.group.startsWith("_")
        || clientDetails.clientId.startsWith("perf")
        || clientDetails.clientId.startsWith("console")) "text-muted"

      else ""
    }
  }

}
