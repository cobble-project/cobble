Jekyll::Hooks.register :site, :after_init do |site|
  site.config["docs_root_label"] = (site.config["docs_root_label"] || "latest").to_s

  commit_label = site.config["docs_commit_label"].to_s.strip
  if commit_label.empty?
    commit = `git rev-parse --short=8 HEAD 2>/dev/null`.strip
    site.config["docs_commit_label"] = commit unless commit.empty?
  end
end
