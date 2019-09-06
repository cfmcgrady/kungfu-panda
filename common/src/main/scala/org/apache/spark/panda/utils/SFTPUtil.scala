package org.apache.spark.panda.utils

import com.jcraft.jsch.OpenSSHConfig
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.FileSystemFile

/**
 * @time 2019-09-06 10:27
 * @author fchen <cloud.chenfu@gmail.com>
 * reference: https://www.baeldung.com/java-file-sftp
 */
object SFTPUtil {

  /**
   * download the file or directory from target SFTP server.
   * @param host the target host to download the file from.
   * @param remoteFile the file or directory of the remote host, notice that default root path is user home.
   * @param localDirectory the local directory prefix that we put the downloaded file.
   */
  def download(host: String,
               remoteFile: String,
               localDirectory: String): Unit = {
//    val configs = OpenSSHConfig.parseFile("~/.ssh/config")
    val userHomePath = System.getProperty("user.home")
    val configs = OpenSSHConfig.parseFile(s"${userHomePath}/.ssh/config")
    val ssh = new SSHClient()
    ssh.loadKnownHosts()
    ssh.addHostKeyVerifier(new PromiscuousVerifier())

//    val path = "/Users/fchen/.ssh/dxy"
    val config = configs.getConfig(host)
    val key = ssh.loadKeys(config.getValue("IdentityFile").replaceFirst("~", userHomePath))
    val hostname = config.getHostname
    val port = config.getPort
    ssh.connect(hostname, port)
    ssh.authPublickey(config.getUser, key)
    val sftp = ssh.newSFTPClient()
    sftp.get(remoteFile, new FileSystemFile(localDirectory))
    sftp.close()
    ssh.disconnect()
    ssh.close()
  }

}
