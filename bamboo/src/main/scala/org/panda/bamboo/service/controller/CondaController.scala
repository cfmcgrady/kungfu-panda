package org.panda.bamboo.service.controller

import java.nio.file.{Files, Paths}
import java.util.{Base64, HashMap => JMap}

import scala.util.control.NonFatal

import org.apache.catalina.servlet4preview.http.HttpServletRequest
import org.apache.spark.panda.utils.Conda
import org.panda.bamboo.util.{CacheKey, CacheManager}
import org.slf4j.LoggerFactory
import org.springframework.core.io.{Resource, UrlResource}
import org.springframework.http.{HttpHeaders, MediaType, ResponseEntity}
import org.springframework.web.bind.annotation.{PathVariable, PostMapping, RequestBody, RequestMapping, RequestMethod, RequestParam, RestController}
import org.springframework.web.multipart.MultipartFile
import org.springframework.web.servlet.mvc.support.RedirectAttributes

/**
 * @time 2019-08-30 10:23
 * @author fchen <cloud.chenfu@gmail.com>
 */
@RestController
@RequestMapping(value = Array("/api/v1/conda"))
class CondaController {

  private val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  /**
   * post method for createAndGet.
   * @param yaml
   * @param request
   * @return
   */
  @RequestMapping(value = Array("/createAndGet"), method = Array(RequestMethod.POST))
  def createAndGet(@RequestBody yaml: String, request: HttpServletRequest): ResponseEntity[Resource] = {

    // scalastyle:off println
    println(yaml)
    // scalastyle:on
    val name = CacheManager.get(key(yaml))
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

  /**
   * get method for createAndGet.different from post method, in get method you should encode conda
   * configurations by base64.
   * @param yaml
   * @param request
   * @return
   */
  @RequestMapping(value = Array("/createAndGet"), method = Array(RequestMethod.GET))
  def createAndGet2(@RequestParam yaml: String, request: HttpServletRequest): ResponseEntity[Resource] = {
    val decodeYamlString = new String(Base64.getDecoder.decode(yaml), "utf-8")
    createAndGet(decodeYamlString, request)
  }

  /**
   * download the environment package by given conda yaml configuration.
   * @param yaml the base64 code of the conda yaml configurations.
   * @param filename
   * @param request
   * @return
   */
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
      val name = CacheManager.get(key(yaml))
      Response(data = Map("name" -> name))
    } catch {
      case NonFatal(e) =>
        Response(stat = false, message = e.getMessage)
    }
  }

  /**
   * direct get environment package by filename.
   * @param filename the md5 code of this package.
   * @return
   */
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

  @RequestMapping(value = Array("/admin/remove/{md5}"), method = Array(RequestMethod.DELETE))
  def remove(@PathVariable md5: String): Response = {
    try {
      CacheManager.remove(CacheKey(md5, new JMap[String, Object]()))
      Response()
    } catch {
      case t: Throwable =>
        logger.error(s"catch an exception when remove environment $md5", t)
        Response(stat = false, message = t.getMessage)
    }
  }

  @PostMapping(value = Array("admin/upload"))
  def manullyUpload(@RequestParam("file") file: MultipartFile,
                    redirectAttributes: RedirectAttributes): Unit = {
    if (file.isEmpty()) {
        redirectAttributes.addFlashAttribute("message", "Please select a file to upload")
        return "redirect:uploadStatus"
    }
    try {
        // Get the file and save it somewhere
        val bytes = file.getBytes()
        val path = Paths.get("/tmp/dd" + file.getOriginalFilename())
        Files.write(path, bytes)
        redirectAttributes.addFlashAttribute("message",
                "You successfully uploaded '" + file.getOriginalFilename() + "'")
    } catch {
      case e => e.printStackTrace()
    }
    return "redirect:/uploadStatus";
  }

  private def key(yaml: String): CacheKey = {
    val ymap = Conda.normalize(yaml)
    CacheKey(ymap.getOrDefault("name", "").asInstanceOf[String], ymap)
  }
}

case class Response(stat: Boolean = true,
                    message: String = "",
                    data: Any = null)
