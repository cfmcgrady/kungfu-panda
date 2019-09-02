package org.panda.bamboo.service.controller

import java.util.Base64

import scala.util.control.NonFatal

import org.apache.catalina.servlet4preview.http.HttpServletRequest
import org.panda.bamboo.util.CacheManager
import org.springframework.core.io.{Resource, UrlResource}
import org.springframework.http.{HttpHeaders, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation.{PathVariable, RequestBody, RequestMapping, RequestMethod, RequestParam, RestController}

/**
 * @time 2019-08-30 10:23
 * @author fchen <cloud.chenfu@gmail.com>
 */
@RestController
@RequestMapping(value = Array("/api/v1/conda"))
class CondaController {

  @RequestMapping(value = Array("/createAndGet"), method = Array(RequestMethod.POST))
  def createAndGet(@RequestBody yaml: String, request: HttpServletRequest): ResponseEntity[Resource] = {

    // scalastyle:off println
    println(yaml)
    // scalastyle:on
    val name = CacheManager.get(yaml)
    val resource = new UrlResource(CacheManager.getFileByName(name).toUri)

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

  @RequestMapping(value = Array("/createAndGet"), method = Array(RequestMethod.GET))
  def createAndGet2(@RequestParam yaml: String, request: HttpServletRequest): ResponseEntity[Resource] = {
    val decodeYamlString = new String(Base64.getDecoder.decode(yaml), "utf-8")
    createAndGet(decodeYamlString, request)
  }

  @RequestMapping(value = Array("/createAndGet/{yaml}/{filename}"), method = Array(RequestMethod.GET))
  def createAndGet3(@PathVariable yaml: String,
                    @PathVariable filename: String,
                    request: HttpServletRequest): ResponseEntity[Resource] = {
    val decodeYamlString = new String(Base64.getDecoder.decode(yaml), "utf-8")
    createAndGet(decodeYamlString, request)
  }

  @RequestMapping(value = Array("/create"), method = Array(RequestMethod.POST))
  def create(@RequestBody yaml: String): Response = {
    try {
      val name = CacheManager.get(yaml)
      Response(data = Map("name" -> name))
    } catch {
      case NonFatal(e) =>
        Response(stat = false, message = e.getMessage)
    }
  }

  @RequestMapping(value = Array("/directGet/{filename}"), method = Array(RequestMethod.GET))
  def directGet(@PathVariable filename: String,
                request: HttpServletRequest): ResponseEntity[Resource] = {

    val file = s"file:///tmp/cache/${filename}/${filename}.tgz"
    val resource = new UrlResource(file)
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

  /**
   * this method is used for manually test.
   * @param yaml
   * @return
   */
  @RequestMapping(value = Array("/encode"), method = Array(RequestMethod.POST))
  def encode(@RequestBody yaml: String): Response = {
    Response(data = Map("result" -> Base64.getEncoder.encodeToString(yaml.getBytes("utf-8"))))
  }
}

case class Response(stat: Boolean = true,
                    message: String = "",
                    data: Any = null)
