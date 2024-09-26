package com.growse.lmdb_kt.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.groups.OptionGroup
import com.github.ajalt.clikt.parameters.groups.provideDelegate
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.path
import com.growse.lmdb_kt.Environment
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.StandardOpenOption
import java.util.*
import kotlin.io.path.writeBytes

private val logger = KotlinLogging.logger {}

enum class Command { Stat }

class CommonOptions : OptionGroup("Standard Options:") {
	val databasePath by option(
		"-d",
		"--database",
		help = "Path to the lmdb database directory",
	).path(mustExist = true, canBeFile = false, mustBeReadable = true).required()
	val debug: Boolean by option(help = "Enable debug logging", envvar = "DEBUG_LOG").flag()
}

class Cli : CliktCommand() {
	override fun run() {
	}
}

class Stat : CliktCommand(name = "stat") {
	private val commonOptions by CommonOptions()
	override fun run() {
		if (commonOptions.debug) {
			System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug")
		}
		println("Database at ${commonOptions.databasePath}")
		Environment(commonOptions.databasePath, readOnly = true, locking = false).stat().run {
			println(
				"""
						Pagesize:	$pageSize
						Entry count:	$entriesCount
						Branch pages: 	$branchPagesCount
						Leaf pages:	$leafPagesCount
						Overflow pages:	$overflowPagesCount
						Tree depth:	$treeDepth
				""".trimIndent(),
			)
		}
	}
}

class Dump : CliktCommand(name = "dump") {
	private val commonOptions by CommonOptions()
	override fun run() {
		if (commonOptions.debug) {
			System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "debug")
		}
		println("Database at ${commonOptions.databasePath}")
		Environment(commonOptions.databasePath, readOnly = true, locking = false).use {
			it.beginTransaction().dump().forEach { (k, v) ->
				commonOptions.databasePath.resolve("${Base64.getEncoder().encodeToString(k.bytes)}.dat").writeBytes(
					v,
					StandardOpenOption.CREATE_NEW,
					StandardOpenOption.WRITE,
					StandardOpenOption.SYNC,
				)
			}
		}
	}
}

fun main(args: Array<String>): Unit = Cli().subcommands(Stat(), Dump()).main(args)
