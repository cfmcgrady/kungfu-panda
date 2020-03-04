package org.panda.bamboo.service.controller

import java.net.URI
import java.nio.file.Paths
import javax.servlet.http.HttpServletRequest

import org.panda.bamboo.util.{CacheManager, MLFlowRunCacheKey}
import org.springframework.core.io.{Resource, UrlResource}
import org.springframework.http.{HttpHeaders, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestMethod, RestController}

/**
 * @time 2019-09-12 14:33
 * @author fchen <cloud.chenfu@gmail.com>
 */
@RestController
@RequestMapping(value = Array("/api/v1/artifact"))
class MLFlowArtifactController {

  private lazy val ARTIFACT_ROOT = new URI(sys.env.getOrElse("MLFLOW_ARTIFACT_ROOT", throw new RuntimeException("")))

  @RequestMapping(value = Array("/createAndGet/{runid}/{filename}"), method = Array(RequestMethod.GET))
  def createAndGet(@PathVariable runid: String,
                   @PathVariable filename: String,
                   request: HttpServletRequest): ResponseEntity[Resource] = {

    val path = CacheManager.get(key(runid))

    val resource = new UrlResource(Paths.get(path).toUri)

    var contentType = ""

    try {
      contentType = request.getServletContext().getMimeType(resource.getFile().getAbsolutePath());
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    // Fallback to the default content type if type could not be determined
    if(contentType == null) {
      contentType = "application/octet-stream"
    }

    ResponseEntity.ok()
      .contentType(MediaType.parseMediaType(contentType))
      .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
      .body(resource)
  }

  @RequestMapping(value = Array("/admin/remove/{runid}"), method = Array(RequestMethod.DELETE))
  def remove(@PathVariable runid: String): Response = {
    try {
      CacheManager.remove(key(runid))
      Response()
    } catch {
      case e: Exception =>
        Response(stat = false, message = e.getMessage)
    }
  }

  private def key(runid: String): MLFlowRunCacheKey = {
    MLFlowRunCacheKey(runid)
  }

}
